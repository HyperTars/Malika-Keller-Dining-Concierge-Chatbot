import * as  AWS from 'aws-sdk';
import * as assert from 'assert';
import * as elasticsearch from 'elasticsearch';
import * as connectionClass from 'http-aws-es';

AWS.config.update({ region: 'us-east-1' });
const QueueUrl = 'https://sqs.us-east-1.amazonaws.com/178190676612/asg1-q1';
const sqs = new AWS.SQS();
const es = new elasticsearch.Client({
    host: 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com',
    connectionClass,
    amazonES: { credentials: new AWS.SharedIniFileCredentials() },
} as any);
const TableName = 'Yelp_Restaurants';
const dynamodb = new AWS.DynamoDB.DocumentClient();
const sns = process.env['SNS_ACC_KEY_ID'] && process.env['SNS_ACC_KEY_SEC'] ? new AWS.SNS({
    // new accounts may have a limit on sms available per day (15)
    // workaround: use multiple accounts
    credentials: new AWS.Credentials(process.env['SNS_ACC_KEY_ID'], process.env['SNS_ACC_KEY_SEC'])
}) : new AWS.SNS();
sns.setSMSAttributes({ attributes: { 'DefaultSMSType': 'Transactional' } });

class MessageAttributesBackend {
    phoneNumberDedup?: Set<string>
    Cuisine?: string
    Location?: string
    Name?: string
    NumberOfPeople?: string
    DiningDate?: string
    DiningTime?: string
    PhoneNumber?: string
    Responses?: AWS.DynamoDB.DocumentClient.ItemList

    async process({ MessageAttributes, phoneNumberDedup }: {
        MessageAttributes?: AWS.SQS.MessageBodyAttributeMap,
        phoneNumberDedup?: Set<string>
    }) {
        assert(MessageAttributes, 'ERR_MESSAGE_ATTR_MISSING');

        const Cuisine = this.Cuisine = MessageAttributes.Cuisine?.StringValue;
        const Location = this.Location = MessageAttributes.Location?.StringValue;
        const Name = this.Name = MessageAttributes.Name?.StringValue;
        const NumberOfPeople = this.NumberOfPeople = MessageAttributes.NumberOfPeople?.StringValue;
        const DiningDate = this.DiningDate = MessageAttributes.DiningDate?.StringValue;
        const DiningTime = this.DiningTime = MessageAttributes.DiningTime?.StringValue;
        const PhoneNumber = this.PhoneNumber = MessageAttributes.PhoneNumber?.StringValue;
        phoneNumberDedup = phoneNumberDedup ?? this.phoneNumberDedup;

        assert(PhoneNumber, 'ERR_MESSAGE_ATTR_PHONENUM');
        assert(!phoneNumberDedup?.has(PhoneNumber), 'ERR_PHONENUM_DUP');
        phoneNumberDedup?.add(PhoneNumber);

        assert(Cuisine && Location && Name && NumberOfPeople && DiningDate && DiningTime, 'ERR_MESSAGE_ATTR_MISC');

        const { hits: { hits } } = await es.search({
            index: 'restaurants',
            size: 3,
            body: { query: { fuzzy: { Cuisine } } },
            ignore: [404],
            maxRetries: 3,
        });
        assert(hits.length, 'ERR_ESEARCH_RESP_EMPTY');

        const RequestItems = {
            [TableName]: {
                Keys: hits.map(({ _source: { RestaurantID } }: any) => ({ RestaurantID }))
            }
        };
        const { Responses } = await dynamodb.batchGet({ RequestItems }).promise();
        assert(Responses, 'ERR_DYNAMO_RESP_MISSING');
        assert(Responses[TableName]?.length, 'ERR_DYNAMO_RESP_EMPTY');

        return this.Responses = Responses[TableName];
    }

    async sendSMS() {
        const { Responses, Cuisine, NumberOfPeople, DiningDate, DiningTime } = this;
        const PhoneNumber = this.PhoneNumber && `+1${this.PhoneNumber}`
        assert(Responses, 'ERR_BACKEND_UNPROCESSED');
        assert(PhoneNumber, 'ERR_MESSAGE_ATTR_PHONENUM');
        assert(Cuisine && NumberOfPeople && DiningDate && DiningTime, 'ERR_MESSAGE_ATTR_MISC');

        const Message = [
            `Hello! Here are my ${Cuisine} restaurant suggestions for ${NumberOfPeople} people, for ${DiningDate} at ${DiningTime}:`,
            ...Responses
                .sort((a, b) => b.Rating - a.Rating)
                .map(({ Name, Rating, Address }, i) =>
                    `${i + 1}: ${Name}(${'★'.repeat(Rating)}${'☆'.repeat(Rating % 1 / 0.5)}), located at ${Address}`
                ),
            'Enjoy your meal!',
        ].join('\n');

        return sns.publish({ Message, PhoneNumber }).promise();
    }

    async sendErrorSMS(Responses: assert.AssertionError) {
        const PhoneNumber = this.PhoneNumber && `+1${this.PhoneNumber}`

        switch (Responses.message) {
            default:
            case 'ERR_MESSAGE_ATTR_MISSING':
            case 'ERR_MESSAGE_ATTR_PHONENUM':
            case 'ERR_MESSAGE_ATTR_MISC': {
                const Message = `Sorry! The chatbot is out of service currently. Please try again later.`;
                return PhoneNumber && sns.publish({ Message, PhoneNumber }).promise();
            }

            case 'ERR_PHONENUM_DUP': {
                const Message = `Sorry! The chatbot queue is cluttered.`;
                return;
            }

            case 'ERR_ESEARCH_RESP_EMPTY': {
                const Message = `Sorry! We cannot find your cuisine ${this.Cuisine ?? 'UNKNOWN'} in our search engine. Please try another one`;
                return PhoneNumber && sns.publish({ Message, PhoneNumber }).promise();
            }

            case 'ERR_DYNAMO_RESP_MISSING':
            case 'ERR_DYNAMO_RESP_EMPTY': {
                const Message = `Sorry! We cannot find your cuisine ${this.Cuisine ?? 'UNKNOWN'} in our database. Please try another one`;
                return PhoneNumber && sns.publish({ Message, PhoneNumber }).promise();
            }
        }
    }
}

const handleLambda = async () => {
    // 1. peek queue
    const { Messages } = await sqs.receiveMessage({
        QueueUrl,
        MessageAttributeNames: ["All"],
        // dedup
        VisibilityTimeout: 30,
        // batch
        MaxNumberOfMessages: 10,
    }).promise();

    // 2. no message => skip
    if (!Messages) return;

	// 3. initialize phone number dedup set
    const phoneNumberDedup = new Set<string>();

	// 4. process all messages simultaneously
    return Promise.all(Messages.map(async ({ MessageAttributes, ReceiptHandle }) => {
        const backend = new MessageAttributesBackend();
        try {
            await backend.process({ MessageAttributes, phoneNumberDedup });
            await backend.sendSMS();

            if (ReceiptHandle) {
                await sqs.deleteMessage({ QueueUrl, ReceiptHandle }).promise();
            }
        }
        catch (e) {
            if (e instanceof assert.AssertionError) {
                await backend.sendErrorSMS(e);

                if (ReceiptHandle) {
                    await sqs.deleteMessage({ QueueUrl, ReceiptHandle }).promise();
                }
            }
            throw e;
        }
    }));
};

export { handleLambda };

if (module && !module.parent) {
    handleLambda();
}

import json
import boto3
from botocore.vendored import requests
import os
import subprocess 

################################ SQS MODULE #######################

def sqs_service():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/899580659559/dining_bot_queue'
    # Receive message from SQS queue
    rec_msg = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    return rec_msg

def del_sqs_service(rec_msg):
    sqs = boto3.client('sqs')
    message = rec_msg['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    # Delete received message from queue
    queue_url = 'https://sqs.us-east-1.amazonaws.com/899580659559/dining_bot_queue'
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

############################### ELASTIC SEARCH MODULE ########################

def es_service(rec_msg):
    es_url = 'https://search-yelp-search-7ha4p5bd7kh65doy2drms2kupu.us-east-1.es.amazonaws.com/restaurants/_search?'
    headers = { "Content-Type": "application/json" }
    input_cuisine = rec_msg['Messages'][0]['MessageAttributes']['Cuisine']['StringValue']  
    es_query = {"size": 100,
             "query": {
             "query_string": {
                "default_field": "Cuisine",
                "query": input_cuisine}
                        }
            }
    r = requests.get(es_url,headers=headers, data=json.dumps(es_query))
    res = r.json()['hits']['hits']
    return res
    
    
################## DYNAMO_DB ####################################

from boto3.dynamodb.conditions import Key, Attr


def dynamo_serv(res, rec_msg):

    business_ids = []
    for i in range(len(res)):
        business_ids.append(res[i]['_source']['Business_ID'])
    
    nbhd_zips = {'harlem':[10026, 10027, 10025, 10030, 10037, 10039, 10029, 10035],
                'chelsea':[10001, 10011, 10018, 10019, 10020, 10036],
                'greenwich village':[10012, 10013, 10014],
                'soho':[10012, 10013, 10014],
                'lower manhattan':[10004, 10005, 10006, 10007, 10038, 10280],
                'lower east hide':[10002, 10003, 10009],
                'upper east side':[10021, 10028, 10044, 10065, 10075, 10128],
                'upper west side':[10023, 10024, 10025],
                'washington heights':[10031, 10032, 10033, 10034, 10040]
                }
    
    location = rec_msg['Messages'][0]['MessageAttributes']['Location']['StringValue']
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    output_rests = []
    for ids in business_ids:
        item = table.get_item(Key={"Business_ID": ids})
        if int(item['Item']['Zip Code']) in nbhd_zips[location]:
            output_rests.append(item)
        if len(output_rests) > 5:
            break
    input_cuisine = rec_msg['Messages'][0]['MessageAttributes']['Cuisine']['StringValue'] 
    if len(output_rests) <= 5:
        for i in range(3):
            ids = business_ids[i]
            output_rests.append(table.get_item(Key={"Business_ID": ids}))
        print('NOTHING')
        intro = 'Here are a few options for ' + input_cuisine + ' food in ' + 'Manhattan ' + ': \n'
    else:
        intro = 'Here are a few options for ' + input_cuisine + ' food in ' + location + ': \n'
    
    name = 'Hello ' + rec_msg['Messages'][0]['MessageAttributes']['Name']['StringValue'] + '!\n'
    opt1 = '1. ' + output_rests[0]['Item']['Name'] +  ', located at ' + output_rests[0]['Item']['Address'] + '\n' 
    opt2 = '2. ' + output_rests[1]['Item']['Name'] +  ', located at ' + output_rests[1]['Item']['Address'] + '\n'
    opt3 = '3. ' + output_rests[2]['Item']['Name'] +  ', located at ' + output_rests[2]['Item']['Address'] + '\n'
    fin = 'Enjoy your meal!'
    text_msg = name + intro + opt1 + opt2 + opt3 + fin
    
    return text_msg

########################### SMS ##############################

def send_sns(rec_msg, text_msg):
    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(
        PhoneNumber= "1" + rec_msg['Messages'][0]['MessageAttributes']['PhoneNumber']['StringValue'],
        Message=text_msg
    )


def lambda_handler(event, context):
    
    rec_msg = sqs_service()
    res = es_service(rec_msg)
    text_msg = dynamo_serv(res, rec_msg)
    send_sns(rec_msg, text_msg)
    del_sqs_service(rec_msg)

    return {
        'statusCode': 200,
        'body': json.dumps('Finished')
    }

import json
import dateutil.parser
import datetime
import time
import os
import math
import boto3

sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/178190676612/asg1-q1'

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    return dispatch(event)
    
def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'DiningSuggestionsIntent':
        location = get_slots(intent_request)["location"]
        cuisine_type = get_slots(intent_request)["cuisine"]
        date = get_slots(intent_request)["dinning_date"]
        time = get_slots(intent_request)["dinning_time"]
        number_of_people = get_slots(intent_request)["number_of_people"]
        name = get_slots(intent_request)["name"]
        phone_number = get_slots(intent_request)["phone_number"]

        source = intent_request['invocationSource']
        if source =='DialogCodeHook':
            slots = get_slots(intent_request)
            validation_result = validate_slots(location,cuisine_type,date,time,number_of_people,name,phone_number)
            if not validation_result['isValid']:
                slots[validation_result['violatedSlot']] = None
                return elicit_slot(intent_request['sessionAttributes'],
                                intent_request['currentIntent']['name'],
                                slots,
                                validation_result['violatedSlot'],
                                validation_result['message'])
            output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

            return delegate(output_session_attributes, get_slots(intent_request))
            
        slots = get_slots(intent_request)   
        send_sqs(slots)
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'Thank you! I will send some recommendations to the phone number: {} later.'.format(phone_number)})


    if intent_name == 'GreetingIntent':
        session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return elicit_intent(
            session_attributes,
            {
                'contentType': 'PlainText',
                'content': 'Hello! What can I help you?'
            }
        )


    if intent_name == 'ThankYouIntent':
        session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return close(
            session_attributes,
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': 'Happy to help. Have a great day!'
            }
        )

    raise Exception(intent_name + ' is not supported!')    


def validate_slots(location,cuisine_type,date,time,number_of_people,name,phone_number):
    
    manhattan_places = ['harlem','chelsea','greenwich village','soho','lower manhattan','lower east hide','upper east side','upper west side','washington heights']
    if location is not None and location.lower() not in manhattan_places:
        return build_validation_result(False,
                                       'location',
                                       'We currently do not support {} as destination. Please enter another neighborhood in Manhattan'.format(location))
        
    cuisines = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza', 'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian', 'seafood', 'dessert']
    if cuisine_type is not None and cuisine_type.lower() not in cuisines:
        return build_validation_result(False,
                                       'cuisine',
                                       'We currently do not support for {} food. Please enter another type of cuisine'.format(cuisine_type))
    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'dinning_date', 'Invalid date! Sample input format: today')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dinning_date', 'We have already passed this day! Please enter a valid date.')
    if time is not None:
        if len(time) != 5:
            return build_validation_result(False, 'dinning_time', "Invalid time! Sample input format: 6pm.")
        for i in range(len(time)):
            if i == 2:
                if time[i] != ":":
                    return build_validation_result(False, 'dinning_time', "Invalid time! Sample input format: 6pm.")
            else:
                if not time[i].isalnum():
                    return build_validation_result(False, 'dinning_time', "Invalid time! Sample input format: 6pm.")

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'dinning_time', "Invalid time! Sample input format: 6pm.")
    if phone_number is not None:
        phone = phone_number.replace('-', '')
        if len(phone) != 10:
            return build_validation_result(False, 'phone_number', "Please enter a 10-digit phone number!")
        for i in phone:
            if not i.isalnum():
                return build_validation_result(False, 'phone_number', "Please do not enter non-digit characters!")

    return build_validation_result(True, None, None)

def send_sqs(slots):
    sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = 1,
        MessageAttributes = {
            'Cuisine': {
                'DataType': 'String',
                'StringValue': slots['cuisine']
            },
            'Location': {
                'DataType': 'String',
                'StringValue': slots['location']
            },
            'Name': {
                'DataType': 'String',
                'StringValue': slots['name']
            },
            'PhoneNumber': {
                'DataType': 'String',
                'StringValue': slots['phone_number']
            },
            'NumberOfPeople': {
                'DataType': 'String',
                'StringValue': slots['number_of_people']
            },
            'DiningDate': {
                'DataType': 'String',
                'StringValue': slots['dinning_date']
            },
            'DiningTime': {
                'DataType': 'String',
                'StringValue': slots['dinning_time']
            }
        },
        MessageBody = (
            'Restauraunt Request Information'
        )
    )


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')    

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def elicit_intent(session_attributes, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitIntent',
            'message': message
        }
    }

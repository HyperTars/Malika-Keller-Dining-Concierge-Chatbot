import json
import dateutil.parser
import datetime
import time
import os
import math
import random
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/899580659559/dining_bot_queue'


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
    
def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GetRestaurant':
        return get_restaurant(intent_request)
    if intent_name == 'Greeting':
        return greeting(intent_request)
    if intent_name == 'Thanking':
        return thanking(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')
    
    
    
def thanking(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Happy to help. Have a great day!'
        }
    )
    
def greeting(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return elicit_intent(
        session_attributes,
        {
            'contentType': 'PlainText',
            'content': 'How can I be of assistance to you today?'
        }
    )

def get_restaurant(intent_request):
    
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
                  'content': 'Thank you! I have collected a few trendy options for you, I will sent detailed information to your phone:{}'.format(phone_number)})


def validate_slots(location,cuisine_type,date,time,number_of_people,name,phone_number):
    
    manhattan_places = ['harlem','chelsea','greenwich village','soho','lower manhattan','lower east hide','upper east side','upper west side','washington heights']
    if location is not None and location.lower() not in manhattan_places:
        return build_validation_result(False,
                                       'location',
                                       'We currently do not support {} as a valid destination. Please enter a neighborhood in Manhattan'.format(location))
        
    cuisines = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza', 'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian', 'seafood', 'dessert']
    if cuisine_type is not None and cuisine_type.lower() not in cuisines:
        return build_validation_result(False,
                                       'cuisine',
                                       'We currently do not have any good options for {} food.  Can you try a different cuisine?'.format(cuisine_type))
    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'dinning_date', 'I did not understand that, when would you like to have your meal?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dinning_date', 'Whoopsie! We\'re already past that date. Please enter a valid date!')
    if time is not None:
        if len(time) != 5:
            return build_validation_result(False, 'dinning_time', "Please input a valid time like 12:00.")
        for i in range(len(time)):
            if i == 2:
                if time[i] != ":":
                    return build_validation_result(False, 'dinning_time', "Please input a valid time like 12:00.")
            else:
                if not time[i].isalnum():
                    return build_validation_result(False, 'dinning_time', "Please input a valid time like 12:00.")

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'dinning_time', None)
    # if name is  None:
    #     return build_validation_result(False, 'name', "Please input a valid name.")
    if phone_number is not None:
        phone = phone_number.replace('-', '')
        if len(phone) != 10:
            return build_validation_result(False, 'phone_number', "Please input a valid 10 digit phone number like 111-111-1111.")
        for i in phone:
            if not i.isalnum():
                return build_validation_result(False, 'phone_number', "Please do not input non-number character for phone number.")

    return build_validation_result(True, None, None)









def send_sqs(slots):
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=1,
        MessageAttributes={
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
            }
        },
        MessageBody=(
            'Restauraunt Request Information'
        )
    )


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot}
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
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }
def elicit_intent(session_attributes, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitIntent',
            'message': message
        }
    }

    return response
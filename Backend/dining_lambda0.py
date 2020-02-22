import json
import boto3
import time
import os
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    logger.debug("lambda0 start")
    
    user_id, text = get_info_from_request(event)
    logger.debug('user_id: {}, text: {}'.format(user_id,text))
    if user_id is None or text is None:
        return get_error_response("system error: Unable to get user Id and text from request")
    chatbot_text = get_chatbot_response(user_id,text)
    logger.debug('chatbot response: {}'.format(chatbot_text))
    if chatbot_text is None:
        return get_error_response("system error: failed in connection with lex")
    else:
        return get_success_response(chatbot_text,user_id)
    
    # return event

def get_info_from_request(event):
    # if "body" not in event:
    #     logger.error("event type error")
    #     return None,None,-1
    # body = event["body"]
    body = event
    if "messages" not in body:
        logger.error("body type error")
        return None,None
    messages = event["messages"]
    if not isinstance(messages,list) or len(messages) < 1:
        logger.error("messages type error or no message")
        return None,None
    message = messages[0]
    if "unconstructed" not in message:
        logger.error("message missing unconstructed")
        return None,None
    if "text" not in message["unconstructed"] or "user_id" not in message["unconstructed"]:
        logger.error("message nissing text or user id")
        return None,None
    user_id = message["unconstructed"]["user_id"]
    text = message["unconstructed"]["text"]
    return user_id,text

def get_error_response(text):
    
    body = {
        "messages":[
            {
                "type":"responce message",
                "unconstructed":{
                    "user_id": None,
                    "text": text,
                    "time":time.time(),
                }
            }]
    }
    
    
    response = {
        "status code":200,
        "body":body
    }
    return response
    
def get_success_response(text,user_id):
        
    body = {
        "messages":[
            {
                "type":"responce message",
                "unconstructed":{
                    "user_id": user_id,
                    "text": text,
                    "time":time.time()
                }
            }]
    }
    
    
    response = {
        "status code":200,
        "body":body
    }
    return response
    
def get_chatbot_response(user_id,text):
    # return "this software is still in develpment, please wait for the update"
    message = ''
    client = boto3.client('lex-runtime')
    lex_response = client.post_text(
        botName='dinningBot',
        botAlias='dinningBot',
        userId=user_id,
        inputText=text
    )
    
    if not isinstance(lex_response,dict):
        logger.error('Lex Response is not a dictionary')
        return None
    
    if 'message' not in lex_response:
        logger.error('Lex Response missing the "message" key')
        return None
        
    message = lex_response['message']
    logger.debug('Lex Message: {}'.format(message))
    return message
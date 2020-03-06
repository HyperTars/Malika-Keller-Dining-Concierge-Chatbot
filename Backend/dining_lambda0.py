import json
import boto3
import time
import os

def lambda_handler(event, context):
    user_id, text = get_info_from_request(event)
    if user_id is None or text is None:
        return get_error_response("Unable to get user ID and text from request")
    chatbot_text = get_chatbot_response(user_id, text)
    if chatbot_text is None:
        return get_error_response("Fail to connect with lex")
    else:
        return get_success_response(chatbot_text, user_id)
    

def get_info_from_request(event):
    body = event
    if "messages" not in body:
        return None, None
    messages = event["messages"]
    if not isinstance(messages,list) or len(messages) < 1:
        return None, None
    message = messages[0]
    if "unconstructed" not in message:
        return None, None
    if "text" not in message["unconstructed"] or "user_id" not in message["unconstructed"]:
        return None, None
    user_id = message["unconstructed"]["user_id"]
    text = message["unconstructed"]["text"]
    return user_id, text

def get_error_response(text):
    body = {
        "messages":[
            {
                "type":"responce message",
                "unconstructed": {
                    "user_id": None,
                    "text": text,
                    "time": time.time(),
                }
            }]
    }
    
    response = {
        "status code": 200,
        "body": body
    }
    return response
    
def get_success_response(text,user_id):
        
    body = {
        "messages":[
            {
                "type":"responce message",
                "unconstructed": {
                    "user_id": user_id,
                    "text": text,
                    "time": time.time()
                }
            }]
    }
    
    
    response = {
        "status code": 200,
        "body": body
    }
    return response
    
def get_chatbot_response(user_id,text):
    message = ''
    client = boto3.client('lex-runtime')
    lex_response = client.post_text(
        botName ='dining_concierge',
        botAlias = 'dining_concierge',
        userId = user_id,
        inputText = text
    )
    
    if not isinstance(lex_response, dict):
        return None
    
    if 'message' not in lex_response:
        return None
        
    message = lex_response['message']
    return message
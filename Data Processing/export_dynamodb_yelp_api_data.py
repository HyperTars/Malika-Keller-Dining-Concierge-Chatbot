from collections import defaultdict
import requests
import csv
import time
from datetime import datetime
import ipdb
from decimal import *
import simplejson as json
import json
import boto3



def check_empty(input):
	if len(str(input)) == 0:
		return 'N/A'
	else:
		return input



dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

#define api key, define the endpoint, and define the header
API_KEY = '7LpwQnUCFqQWIWda4sneoWju2dA5LmXyby1qFNS2V4y6Kn0SHR0iMSUl7KfftSpCeH12_fpzNEaWmRulUXRF5lT90h_2c6ggT6vlxrAvKWhxKBxCuv9GDtbqwE-ZXXYx' 
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/' # + {id}
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

#define parameters
PARAMETERS = {'term': 'food', 
			  'limit': 50,
			  'radius': 15000,
			  'offset': 200,
			  'location': 'Manhattan'}


#make request to yelp API
#response = requests.get(url = ENDPOINT, params =  PARAMETERS, headers=HEADERS)
#convert JSON string to a dictionary
#business_data = response.json()
#print(business_data)


cuisines = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza', 'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian', 'seafood', 'dessert']

manhattan_nbhds = 	['Lower East Side, Manhattan',
					'Upper East Side, Manhattan',
					'Upper West Side, Manhattan',
					'Washington Heights, Manhattan',
					'Central Harlem, Manhattan',
					'Chelsea, Manhattan',
					'Manhattan',
					'East Harlem, Manhattan',
					'Gramercy Park, Manhattan',
					'Greenwich, Manhattan',
					'Lower Manhattan, Manhattan']

start = time.time()
for nbhd in manhattan_nbhds:
	PARAMETERS['location'] = nbhd
	for cuisine in cuisines: 
		PARAMETERS['term'] = cuisine
		
		#make request to yelp API for specified cuisine + location
		response = requests.get(url = ENDPOINT, params =  PARAMETERS, headers=HEADERS)
		business_data = response.json()['businesses']
		for business in business_data:
			now = datetime.now()
			restauraunt_data = {}
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			# #NOTE: to export into dynamoDB, you cannot have float values + u need to load it as a json file; simplejson library supports Decimal() function
			# #make request to yelp api for specific business ID and get its hours of operation 
			# business_response = requests.get(url = ENDPOINT_ID + business['id'], headers=HEADERS)
			# biz = business_response.json()
			# if 'hours' in biz:
			# 	if 'open' in biz['hours'][0]:
			# 		open_dict = biz['hours'][0]['open']
			# else:
			# 	open_dict = 'N/A'
			table.put_item(
			Item = {
				'Business_ID':check_empty(business['id']),
				'insertedAtTimestamp': check_empty(dt_string),
				'Name':  check_empty(business['name']),
				'Cuisine': check_empty(cuisine),
				'Rating': check_empty(Decimal(business['rating'])),
				'Number of Reviews' : check_empty(Decimal(business['review_count'])),
				'Address': check_empty(business['location']['address1']),
				'Zip Code': check_empty(business['location']['zip_code']),
				'Latitude': check_empty(str(business['coordinates']['latitude'])),
				'Longitude': check_empty(str(business['coordinates']['longitude'])),
				'Open': 'N/A'
			}
			)
			#ipdb.set_trace()


	print('Fin ',nbhd, time.time()- start)



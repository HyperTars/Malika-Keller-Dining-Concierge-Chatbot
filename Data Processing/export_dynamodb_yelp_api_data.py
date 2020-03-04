'''
OLD FILE CAN BE DELETED
'''

from collections import defaultdict
import requests
import csv
import time
from datetime import datetime
from decimal import *
import simplejson as json
import json
import boto3


def check_empty(input):
	if len(str(input)) == 0:
		return 'N/A'
	else:
		return input


dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('yelp-restaurants')

# define api key, define the endpoint, and define the header
API_KEY = 'c9R-lxzMB2pLkv_i-3KskCPRTbzj0ilRPFW2NWaUzxph7HSpVW_qBL-vnbvX15O28mJK1x4WpU6MnvVZ8siYfAGN09kN2sPpZpLbzJAMA8-3GbLWiLQAFQ-6Oq5eXnYx' 
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
# replace 'search' with 'Yelp API: Client ID'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/paqI8bHTDXVPNcfhWU1C5w'
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
#filename='yelp_data.json'
#with open(filename,'w') as file_obj:
#	json.dump(business_data,file_obj)


cuisines = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza', 
		'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian', 
		'seafood', 'dessert']

manhattan_nbhds = ['Lower East Side, Manhattan',
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

rest_data = []
headers = ['Business_ID', 'Name', 'Cuisine', 'Rating', 'Number of Reviews', 'Address', \
	'Zip Code', 'Latitude', 'Longitude', 'isClosed', 'insertedAtTimestamp']

start = time.time()
for nbhd in manhattan_nbhds:
	PARAMETERS['location'] = nbhd
	for cuisine in cuisines:
		PARAMETERS['term'] = cuisine
		
		#make request to yelp API for specified cuisine + location
		response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
		business_data = response.json()['businesses']
		for business in business_data:
			now = datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			item = {'Business_ID': check_empty(business['id']),
					'Name': check_empty(business['name']),
					'Cuisine': check_empty(cuisine),
					'Rating': check_empty(Decimal(business['rating'])),
					'Number of Reviews': check_empty(Decimal(business['review_count'])),
					'Address': check_empty(business['location']['address1']),
					'Zip Code': check_empty(business['location']['zip_code']),
					'Latitude': check_empty(str(business['coordinates']['latitude'])),
					'Longitude': check_empty(str(business['coordinates']['longitude'])),
					'isClosed': check_empty(str(business['is_closed'])),
					'insertedAtTimestamp': check_empty(dt_string)}
			rest_data.append(item)
			table.put_item(
				Item = {
					'Business_ID': check_empty(business['id']),
					'Name': check_empty(business['name']),
					'Cuisine': check_empty(cuisine),
					'Rating': check_empty(Decimal(business['rating'])),
					'Number of Reviews': check_empty(Decimal(business['review_count'])),
					'Address': check_empty(business['location']['address1']),
					'Zip Code': check_empty(business['location']['zip_code']),
					'Latitude': check_empty(str(business['coordinates']['latitude'])),
					'Longitude': check_empty(str(business['coordinates']['longitude'])),
					'isClosed': check_empty(str(business['is_closed'])),
					'insertedAtTimestamp': check_empty(dt_string)
				}
			)
			
	print('Fin ',nbhd, time.time() - start)

with open('yelp_restaurants_raw.csv','a+',newline='',encoding='utf-8') as f:
    f_csv = csv.DictWriter(f,headers)
    f_csv.writeheader()
    f_csv.writerows(rest_data)

'''
# JSON

 {'id': 'jW4PUFUQnarsH9K2JgiKuw', 
 'alias': 'bluestone-lane-new-york-25', 
 'name': 'Bluestone Lane', 
 'image_url': 'https://s3-media4.fl.yelpcdn.com/bphoto/Z72vykQg-7l8G5y9qS6gKA/o.jpg', 
 'is_closed': False, 
 'url': 'https://www.yelp.com/biz/bluestone-lane-new-york-25?adjust_creative=paqI8bHTDXVPNcfhWU1C5w&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=paqI8bHTDXVPNcfhWU1C5w', 
 'review_count': 124, 
 'categories': [{
	 'alias': 'coffee', 
	 'title': 'Coffee & Tea'}, 
	 {'alias': 'gluten_free', 
	 'title': 'Gluten-Free'}, 
	 {'alias': 'breakfast_brunch', 
	 'title': 'Breakfast & Brunch'}], 
'rating': 4.0, 
'coordinates': {
	'latitude': 40.78365, 
	'longitude': -73.97773}, 
'transactions': [], 
'price': '$$', 
'location': {
	'address1': '417 Amsterdam Ave', 
	'address2': None, 
	'address3': None, 
	'city': 'New York', 
	'zip_code': '10024', 
	'country': 'US', 
	'state': 'NY', 
	'display_address': [
		'417 Amsterdam Ave', 
		'New York, NY 10024']}, 
'phone': '+17183746858', 
'display_phone': '(718) 374-6858', 
'distance': 7489.614786064939}, 
'''
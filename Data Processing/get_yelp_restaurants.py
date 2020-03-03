import requests
import csv
import time
import boto3
import simplejson as json
import pandas as pd
from datetime import datetime
from decimal import Decimal

# constants
DB_REGION = 'us-east-1'
TABLE_NAME = 'yelp_restaurants'

API_KEY = 'c9R-lxzMB2pLkv_i-3KskCPRTbzj0ilRPFW2NWaUzxph7HSpVW_qBL-vnbvX15O28mJK1x4WpU6MnvVZ8siYfAGN09kN2sPpZpLbzJAMA8-3GbLWiLQAFQ-6Oq5eXnYx'
CLIENT_ID = 'paqI8bHTDXVPNcfhWU1C5w'

ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/' + CLIENT_ID
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

PARAMETERS = {
        'term': 'food',
        'limit': 50,
        'radius': 15000,
        'offset': 200,
        'location': 'Manhattan'}

CUISINES = ['italian', 'chinese', 'mexican', 'american', 'japanese', 'pizza',
        'healthy', 'brunch', 'korean', 'thai', 'vietnamese', 'indian',
        'seafood', 'dessert']

MANHATTAN_AREAS = ['Lower East Side, Manhattan',
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

CSV_HEADERS = ['Business_ID', 'Name', 'Cuisine', 'Rating', 'Number of Reviews', 'Address',
        'Zip Code', 'Latitude', 'Longitude', 'isClosed', 'insertedAtTimestamp']


# check data
def isValid(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input


# write to CSV file
def writeCSV(data):
    with open('yelp_restaurants_raw.csv', 'a+', newline='', encoding='utf-8') as f:
        f_csv = csv.DictWriter(f, CSV_HEADERS)
        f_csv.writeheader()
        f_csv.writerows(data)


dynamodb = boto3.resource('dynamodb', region_name=DB_REGION)
table = dynamodb.Table(TABLE_NAME)
start = time.time()
temp = start

# get data
for area in MANHATTAN_AREAS:
    # itr each area
    PARAMETERS['location'] = area
    temp = time.time()
    area_restaurants = []

    # itr each cuisine
    for cuisine in CUISINES:
        PARAMETERS['term'] = cuisine
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        business_data = response.json()['businesses']

        # resolve request
        for business in business_data:
            time_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            item = {'Business_ID': isValid(business['id']),
                    'Name': isValid(business['name']),
                    'Cuisine': isValid(cuisine),
                    'Rating': isValid(Decimal(business['rating'])),
                    'Number of Reviews': isValid(Decimal(business['review_count'])),
                    'Address': isValid(business['location']['address1']),
                    'Zip Code': isValid(business['location']['zip_code']),
                    'Latitude': isValid(str(business['coordinates']['latitude'])),
                    'Longitude': isValid(str(business['coordinates']['longitude'])),
                    'isClosed': isValid(str(business['is_closed'])),
                    'insertedAtTimestamp': isValid(time_string)}

            # write restaurant data to DynamoDB and local area restaurants list
            area_restaurants.append(item)
            table.put_item(Item=item)

    # finsih area restaurants data
    writeCSV(area_restaurants)
    print('Finish', area, ' Area time spent:', time.time() - temp, ' Total time spent:', time.time() - start)

# delete redundant headers
csv_data = pd.read_csv('yelp_restaurants_raw.csv')
csv_data = csv_data[~csv_data['Business_ID'].str.contains('Business_ID')]
csv_data.to_csv('yelp_restaurants_raw.csv', index=False)

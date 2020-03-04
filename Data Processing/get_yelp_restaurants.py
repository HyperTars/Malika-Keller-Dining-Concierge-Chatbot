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
TABLE_NAME = 'Yelp_Restaurants'
CSV_FILE = 'Yelp_Restaurants.csv'

API_KEY = 'c9R-lxzMB2pLkv_i-3KskCPRTbzj0ilRPFW2NWaUzxph7HSpVW_qBL-vnbvX15O28mJK1x4WpU6MnvVZ8siYfAGN09kN2sPpZpLbzJAMA8-3GbLWiLQAFQ-6Oq5eXnYx'
CLIENT_ID = 'paqI8bHTDXVPNcfhWU1C5w'

CSV_HEADERS = ['RestaurantID', 'Name', 'Cuisine', 'Rating', 'NumberOfReviews', 
        'Address', 'ZipCode', 'Latitude', 'Longitude', 'IsClosed', 'InsertTime']

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


# check data
def isValid(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input


# write to CSV file
def writeCSV(data):
    with open(CSV_FILE, 'a+', newline='', encoding='utf-8') as f:
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
            item = {CSV_HEADERS[0]: isValid(business['id']),
                    CSV_HEADERS[1]: isValid(business['name']),
                    CSV_HEADERS[2]: isValid(cuisine),
                    CSV_HEADERS[3]: isValid(Decimal(business['rating'])),
                    CSV_HEADERS[4]: isValid(Decimal(business['review_count'])),
                    CSV_HEADERS[5]: isValid(business['location']['address1']),
                    CSV_HEADERS[6]: isValid(business['location']['zip_code']),
                    CSV_HEADERS[7]: isValid(str(business['coordinates']['latitude'])),
                    CSV_HEADERS[8]: isValid(str(business['coordinates']['longitude'])),
                    CSV_HEADERS[9]: isValid(str(business['is_closed'])),
                    CSV_HEADERS[10]: isValid(time_string)}

            # write restaurant data to DynamoDB and local area restaurants list
            area_restaurants.append(item)
            table.put_item(Item=item)

    # finsih area restaurants data
    writeCSV(area_restaurants)
    print('Finish', area, ' Area_time_spent:', time.time() - temp, ' Total_time_spent:', time.time() - start)

# delete redundant headers
csv_data = pd.read_csv(CSV_FILE)
csv_data = csv_data[~csv_data[CSV_HEADERS[0]].str.contains(CSV_HEADERS[0])]
csv_data.to_csv(CSV_FILE, index=False)

import requests
import csv
import time
import boto3
import pandas as pd
import os
from datetime import datetime
from decimal import Decimal

# constants #
# AWS config
AWS_DB_REGION = 'us-east-1'
AWS_TABLE_NAME = 'Yelp_Restaurants'
AWS_PRIMARY_KEY = 'RestaurantID'

# local csv config
CSV_FILE = 'Yelp_Restaurants.csv'
CSV_HEAD = [AWS_PRIMARY_KEY, 'Name', 'Cuisine', 'Rating', 'NumberOfReviews',
            'Address', 'ZipCode', 'Latitude', 'Longitude', 'IsClosed',
            'InsertTime']

# Yelp API config
YELP_API_KEY = 'c9R-lxzMB2pLkv_i-3KskCPRTbzj0ilRPFW2NWaUzxph7HSpVW_qBL-vnbvX15O28mJK1x4WpU6MnvVZ8siYfAGN09kN2sPpZpLbzJAMA8-3GbLWiLQAFQ-6Oq5eXnYx'
YELP_CLIENT_ID = 'paqI8bHTDXVPNcfhWU1C5w'

YELP_ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
YELP_ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/' + YELP_CLIENT_ID
YELP_REQ_HEADERS = {'Authorization': 'bearer %s' % YELP_API_KEY}

YELP_REQ_PARAMETERS = {
    'term': 'food',
    'limit': 50,
    'radius': 15000,
    'offset': 200,
    'location': 'Manhattan'}

YELP_REQ_CUISINES = ['italian', 'chinese', 'mexican', 'american', 'japanese',
                     'pizza', 'healthy', 'brunch', 'korean', 'thai',
                     'vietnamese', 'indian', 'seafood', 'dessert']

YELP_REQ_AREAS = ['Lower East Side, Manhattan',
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
def valid(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input


# write to CSV file
def writeCSV(data):
    with open(CSV_FILE, 'a+', newline='', encoding='utf-8') as f:
        f_csv = csv.DictWriter(f, CSV_HEAD)
        f_csv.writeheader()
        f_csv.writerows(data)


# init
dynamodb = boto3.resource('dynamodb', region_name=AWS_DB_REGION)
table = dynamodb.Table(AWS_TABLE_NAME)
if os.path.exists(CSV_FILE):
    os.remove(CSV_FILE)
start = time.time()
temp = start
area_idx = 1
total_item = 0

# get data
for area in YELP_REQ_AREAS:
    # itr each area
    YELP_REQ_PARAMETERS['location'] = area
    temp = time.time()
    area_restaurants = []
    area_item = 0

    # itr each cuisine
    for cuisine in YELP_REQ_CUISINES:
        YELP_REQ_PARAMETERS['term'] = cuisine
        response = requests.get(url=YELP_ENDPOINT,
                                params=YELP_REQ_PARAMETERS,
                                headers=YELP_REQ_HEADERS)
        try:
            business_data = response.json()['businesses']
        except 'businesses' not in response.json():
            print ('Yelp API Request/Return Error')

        business_data = response.json()['businesses']

        # process request
        for data in business_data:
            time_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            item = {CSV_HEAD[0]: valid(data['id']),
                    CSV_HEAD[1]: valid(data['name']),
                    CSV_HEAD[2]: valid(cuisine),
                    CSV_HEAD[3]: valid(Decimal(data['rating'])),
                    CSV_HEAD[4]: valid(Decimal(data['review_count'])),
                    CSV_HEAD[5]: valid(data['location']['address1']),
                    CSV_HEAD[6]: valid(data['location']['zip_code']),
                    CSV_HEAD[7]: valid(str(data['coordinates']['latitude'])),
                    CSV_HEAD[8]: valid(str(data['coordinates']['longitude'])),
                    CSV_HEAD[9]: valid(str(data['is_closed'])),
                    CSV_HEAD[10]: valid(time_string)}

            # write restaurant data to DynamoDB and local area restaurants list
            area_restaurants.append(item)
            table.put_item(Item=item)
            area_item += 1
            total_item += 1

    # finsih area restaurants data
    writeCSV(area_restaurants)
    print ('(%d/11) "%s" finished, item count: %d, time spent: %ds, total time: %ds, total item: %d'
           % (area_idx, area, area_item, int(time.time() - temp), int(time.time() - start), total_item))
    area_idx += 1

# delete redundant headers
csv_data = pd.read_csv(CSV_FILE)
csv_data = csv_data[~csv_data[CSV_HEAD[0]].str.contains(CSV_HEAD[0])]
csv_data.to_csv(CSV_FILE, index=False)

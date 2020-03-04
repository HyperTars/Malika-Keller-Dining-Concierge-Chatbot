import requests
import csv
import time
import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal

# constants #
# AWS config
DB_REGION = 'us-east-1'
TABLE_NAME = 'Yelp_Restaurants'
PRIMARY_KEY = 'RestaurantID'

# local csv config
CSV_FILE = 'Yelp_Restaurants.csv'
CSV_HEAD = [PRIMARY_KEY, 'Name', 'Cuisine', 'Rating', 'NumberOfReviews',
            'Address', 'ZipCode', 'Latitude', 'Longitude', 'IsClosed',
            'InsertTime']

# Yelp API config
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

AREAS = ['Lower East Side, Manhattan',
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
dynamodb = boto3.resource('dynamodb', region_name=DB_REGION)
table = dynamodb.Table(TABLE_NAME)
start = time.time()
temp = start
idx = 1

# get data
for area in AREAS:
    # itr each area
    PARAMETERS['location'] = area
    temp = time.time()
    area_restaurants = []

    # itr each cuisine
    for cuisine in CUISINES:
        PARAMETERS['term'] = cuisine
        response = requests.get(url=ENDPOINT,
                                params=PARAMETERS,
                                headers=HEADERS)
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

    # finsih area restaurants data
    writeCSV(area_restaurants)
    print ('(%d/11) "%s" finished, time spent: %ds, total time: %ds' %
           (idx, area, int(time.time() - temp), int(time.time() - start)))
    idx += 1

# delete redundant headers
csv_data = pd.read_csv(CSV_FILE)
csv_data = csv_data[~csv_data[CSV_HEAD[0]].str.contains(CSV_HEAD[0])]
csv_data.to_csv(CSV_FILE, index=False)

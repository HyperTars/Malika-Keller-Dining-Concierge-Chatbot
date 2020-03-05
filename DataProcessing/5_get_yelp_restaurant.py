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
    'radius': 10000,
    'location': 'Manhattan',
    'sort_by': 'distance'}

YELP_REQ_CUISINES = ['italian', 'chinese', 'mexican', 'american', 'japanese',
                     'pizza', 'healthy', 'brunch', 'korean', 'thai',
                     'vietnamese', 'indian', 'seafood', 'dessert']

YELP_REQ_AREAS = ['Central Harlem, Manhattan',
                  'Washington Heights, Manhattan',
                  'East Harlem, Manhattan',
                  'Upper West Side, Manhattan',
                  'Upper East Side, Manhattan',
                  'Midtown West, Manhattan',
                  'Midtown East, Manhattan',
                  'Chelsea, Manhattan',
                  'Murray Hill, Manhattan',
                  'Gramercy, Manhattan',
                  'Greenwich, Manhattan',
                  'East Village, Manhattan',
                  'Soho, Manhattan',
                  'Lower East Side, Manhattan',
                  'Tribeca, Manhattan',
                  'Chinatown, Manhattan',
                  'Financial District, Manhattan']

AREA_ZIP = {'Central Harlem, Manhattan': [10026, 10027, 10030, 10037, 10039],
            'Washington Heights, Manhattan': [10031, 10032, 10033, 10034, 10040],
            'East Harlem, Manhattan': [10029, 10035],
            'Upper West Side, Manhattan': [10023, 10024, 10025],
            'Upper East Side, Manhattan': [10021, 10028, 10044, 10065, 10075, 10128],
            'Midtown West, Manhattan': [10019, 10020],
            'Midtown East, Manhattan': [10022],
            'Chelsea, Manhattan': [10001, 10011, 10018, 10036],
            'Murray Hill, Manhattan': [10017],
            'Gramercy, Manhattan': [10010, 10016],
            'Greenwich, Manhattan': [10012, 10014],
            'East Village, Manhattan': [10003, 10009],
            'Soho, Manhattan': [10013],
            'Lower East Side, Manhattan': [10002],
            'Tribeca, Manhattan': [10282, 10007],
            'Chinatown, Manhattan': [10038],
            'Financial District, Manhattan': [10006, 10280, 10005, 10004]}


# Functions #
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


def getDataFromYelp():
    # init
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
    print ('=========== start requesting data from Yelp ===========')
    start = time.time()
    area_time = start
    area_idx = 1
    total_item = 0

    # get data
    for area in YELP_REQ_AREAS:
        # itr each area
        YELP_REQ_PARAMETERS['location'] = area
        area_time = time.time()
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

                # write restaurant data to local area restaurants list
                area_restaurants.append(item)
                area_item += 1
                total_item += 1

        # finsih area restaurants data and write to CSV
        writeCSV(area_restaurants)
        print ('(%d/%d) "%s" downloaded, item count: %d, total item: %d, time spent: %ds, total time: %ds' % (
                area_idx, len(YELP_REQ_AREAS), area, area_item, total_item,
                int(time.time() - area_time), int(time.time() - start)))
        area_idx += 1
    print ('=========== requesting data from Yelp done ===========')


# format CSV data: remove duplicates and sort
def formatCSV():
    print ('=========== start formatting data ===========')
    f = pd.read_csv(CSV_FILE)
    print ('Items before format:', len(f))
    f = f[~f[CSV_HEAD[0]].str.contains(CSV_HEAD[0])]
    f.drop_duplicates(subset=[AWS_PRIMARY_KEY], keep='first', inplace=True)
    f.sort_values([CSV_HEAD[2], CSV_HEAD[6]], inplace=True)
    print ('Items after format:', len(f))
    f.to_csv(CSV_FILE, index=False)
    print ('=========== formatting data done ===========')


# upload to DynamoDB
def uploadToDynamoDB():
    print ('=========== start uploading data to DynamoDB ===========')
    # init csv file and AWS
    yelp_csv = pd.read_csv(CSV_FILE)
    dynamodb = boto3.resource('dynamodb', region_name=AWS_DB_REGION)
    table = dynamodb.Table(AWS_TABLE_NAME)

    # init counting var
    cuisine_last = str(yelp_csv[CSV_HEAD[2]][0])
    cuisine_type = 1
    cuisine_count = 0
    total_count = 0
    start_time = time.time()
    point_time = start_time

    # upload
    for i in range(len(yelp_csv)):
        cuisine_curr = str(yelp_csv[CSV_HEAD[2]][i])
        cuisine_count += 1
        total_count += 1
        item = {CSV_HEAD[0]: str(yelp_csv[CSV_HEAD[0]][i]),
                CSV_HEAD[1]: str(yelp_csv[CSV_HEAD[1]][i]),
                CSV_HEAD[2]: str(yelp_csv[CSV_HEAD[2]][i]),
                CSV_HEAD[3]: Decimal(yelp_csv[CSV_HEAD[3]][i].astype(Decimal)),
                CSV_HEAD[4]: Decimal(yelp_csv[CSV_HEAD[4]][i].astype(Decimal)),
                CSV_HEAD[5]: str(yelp_csv[CSV_HEAD[5]][i]),
                CSV_HEAD[6]: str(yelp_csv[CSV_HEAD[6]][i]),
                CSV_HEAD[7]: str(yelp_csv[CSV_HEAD[7]][i]),
                CSV_HEAD[8]: str(yelp_csv[CSV_HEAD[8]][i]),
                CSV_HEAD[9]: str(yelp_csv[CSV_HEAD[9]][i].astype(str)),
                CSV_HEAD[10]: str(yelp_csv[CSV_HEAD[10]][i])}
        table.put_item(Item=item)

        # finish uploading a cuisine type
        if cuisine_curr != cuisine_last:
            now = time.time()
            print ('(%d/%d) cuisine: "%s" uploaded, time spent: %ds, total time: %ds, current item: %d, total item: %d' % (
                cuisine_type, len(YELP_REQ_CUISINES), cuisine_last,
                int(now - point_time), int(now - start_time),
                cuisine_count, total_count))
            point_time = now
            cuisine_last = cuisine_curr
            cuisine_count = 0
            cuisine_type += 1

    # finish uploading last cuisine type
    print ('(%d/%d) cuisine: "%s" uploaded, time spent: %ds, total time: %ds, current item: %d, total item: %d' % (
        cuisine_type, len(YELP_REQ_CUISINES), cuisine_curr,
        int(now - point_time), int(now - start_time),
        cuisine_count, total_count))

    print ('=========== uploading data to DynamoDB done ===========')


# Process #
getDataFromYelp()
formatCSV()
uploadToDynamoDB()

import pandas as pd
import json
import os

YELP_CSV = 'Yelp_Restaurants.csv'
YELP_ES_CSV = 'Yelp_Restaurants_Elastic_Search.csv'
YELP_ES_JSON = 'Yelp_Elastic_Search.json'
AWS_END_POINT = 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com'
# AWS_END_POINT = 'https://search-yelp-restaurants-qbilabfsrw4mpkj7lod4ptr4ye.us-east-2.es.amazonaws.com'
AWS_JSON_KEY = 'Restaurant'
AWS_ES_KEY = 'RestaurantID'
AWS_ES_VAL = 'Cuisine'
XPUT_FILE = 'xput.txt'

yelp_csv = pd.read_csv(YELP_CSV)
yelp_es_list = []


# Write XPUT
def writeXPUT(row):
    with open(XPUT_FILE, 'a+') as f:
        f.write(row)
        f.write('\n')
    f.close


# create csv for elastic search
if os.path.exists(YELP_ES_CSV):
    os.remove(YELP_ES_CSV)
yelp_es_csv = pd.DataFrame()
yelp_es_csv = (yelp_csv.loc[:, [AWS_ES_KEY, AWS_ES_VAL]])
yelp_es_csv.to_csv(path_or_buf=YELP_ES_CSV, index=False)

# create json for elastic search
if os.path.exists(YELP_ES_JSON):
    os.remove(YELP_ES_JSON)
index = 0
for i in range(len(yelp_csv)):
    temp = {}
    temp[AWS_JSON_KEY] = {}
    temp[AWS_JSON_KEY][AWS_ES_KEY] = yelp_csv[AWS_ES_KEY][i]
    temp[AWS_JSON_KEY][AWS_ES_VAL] = yelp_csv[AWS_ES_VAL][i]
    yelp_es_list.append(temp)

with open(YELP_ES_JSON, 'w+') as f:
    # json.dump(yelp_es, f)
    for row in yelp_es_list:
        f.write("{ \"index\" : { \"_index\": \"restaurants\", \"_type\" : \"Restaurant\", \"_id\" : \"")
        f.write(str(index))
        f.write("\" } }\n")
        json.dump(yelp_es_list[index], f)
        f.write("\n")
        index = index + 1
f.close()

# es_CSV = pd
yelp_es_csv = pd.read_csv(YELP_ES_CSV)

# XPUT
if os.path.exists(XPUT_FILE):
    os.remove(XPUT_FILE)
for i in range(len(yelp_es_csv)):
    initial = "curl -XPUT %s/restaurants/%s/%d -d '" % (
        AWS_END_POINT, AWS_JSON_KEY, i + 1)
    middle = '{"%s": "%s", "%s": "%s"}' % (
        AWS_ES_KEY, yelp_es_csv[AWS_ES_KEY][i],
        AWS_ES_VAL, yelp_es_csv[AWS_ES_VAL][i])
    final = "' -H 'Content-Type: application/json'"
    full = initial + middle + final
    writeXPUT(full)
    # os.system(full)


# curl -XPUT https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/Restaurant/1 -d '{"RestaurantID": "RestaurantID", "Cuisine": "Cuisine"}' -H 'Content-Type: application/json'
# curl -XGET 'https://search-yelp-search-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/_search?q=chinese'


'''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
        },
    "Action": [
        "es:*"
    ],
    "Condition": {
'''

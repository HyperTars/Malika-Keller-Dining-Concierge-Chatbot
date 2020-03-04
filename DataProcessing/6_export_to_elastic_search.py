import pandas as pd
import ipdb
import json
import os

YELP_CSV = 'Yelp_Restaurants.csv'
YELP_ES_CSV = 'Yelp_Restaurants_Elastic_Search.csv'
YELP_ES_JSON = 'Yelp_Elastic_Search.json'
END_POINT = 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com'
# END_POINT = 'https://search-yelp-restaurants-qbilabfsrw4mpkj7lod4ptr4ye.us-east-2.es.amazonaws.com'

yelp_csv = pd.read_csv(YELP_CSV)
yelp_es_list = []

# create csv for elastic search
yelp_es_csv = pd.DataFrame()
yelp_es_csv = (yelp_csv.loc[:, ['RestaurantID', 'Cuisine']])
yelp_es_csv.to_csv(path_or_buf=YELP_ES_CSV, index=False)

# create json for elastic search
index = 0
for i in range(len(yelp_csv)):
    temp = {}
    temp['Restaurant'] = {}
    temp['Restaurant']['RestaurantID'] = yelp_csv['RestaurantID'][i]
    temp['Restaurant']['Cuisine'] = yelp_csv['Cuisine'][i]
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

for i in range(len(yelp_es_csv)):
    endpoint = END_POINT + '/restaurants/Restaurant/' + str(i)
    initial = 'curl -XPUT ' + END_POINT + '/restaurants/Restaurant/{} -d '.format(i)
    middle = "'" + "{" + '"RestaurantID": "{}", "Cuisine": "{}"'.format(
            yelp_es_csv['RestaurantID'][i], yelp_es_csv['Cuisine'][i]) + "}" + "' "
    final = "-H " + "'" + "Content-Type: application/json" + "'"
    full = initial + middle + final
    os.system(full)

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

import pandas as pd
import json
import os

# constants #
# Yelp CSV config
YELP_CSV = 'Yelp_Restaurants.csv'
YELP_ES_CSV = 'Yelp_Restaurants_Elastic_Search.csv'
YELP_ES_JSON = 'Yelp_Elastic_Search.json'
# AWS config
AWS_EAST_1 = 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com'
AWS_EAST_2 = 'https://search-yelp-restaurants-qbilabfsrw4mpkj7lod4ptr4ye.us-east-2.es.amazonaws.com'
AWS_ES_END_POINT = AWS_EAST_1
AWS_ES_INDICE = 'restaurants'
AWS_ES_MAP = 'Restaurant'
AWS_ES_KEY = 'RestaurantID'
AWS_ES_VAL = 'Cuisine'
XPUT_FILE = 'xput.txt'


# Write XPUT
def writeXPUT(row):
    with open(XPUT_FILE, 'a+') as f:
        f.write(row)
        f.write('\n')
    f.close


# create csv for elastic search
if os.path.exists(YELP_ES_CSV):
    os.remove(YELP_ES_CSV)
yelp_csv = pd.read_csv(YELP_CSV)
yelp_es_list = []
yelp_es_csv = pd.DataFrame()
yelp_es_csv = (yelp_csv.loc[:, [AWS_ES_KEY, AWS_ES_VAL]])
yelp_es_csv.to_csv(path_or_buf=YELP_ES_CSV, index=False)
yelp_es_csv = pd.read_csv(YELP_ES_CSV)

# create json for elastic search
if os.path.exists(YELP_ES_JSON):
    os.remove(YELP_ES_JSON)
for i in range(len(yelp_csv)):
    temp = {}
    temp[AWS_ES_MAP] = {}
    temp[AWS_ES_MAP][AWS_ES_KEY] = yelp_csv[AWS_ES_KEY][i]
    temp[AWS_ES_MAP][AWS_ES_VAL] = yelp_csv[AWS_ES_VAL][i]
    yelp_es_list.append(temp)
index = 0
with open(YELP_ES_JSON, 'w+') as f:
    # json.dump(yelp_es, f)
    for row in yelp_es_list:
        f.write('{ \"index\" : { \"_index\": \"' + AWS_ES_INDICE
                + '\", \"_type\" : \"'
                + AWS_ES_MAP + '\", \"_id\" : \"')
        f.write(str(index))
        f.write("\" } }\n")
        json.dump(yelp_es_list[index], f)
        f.write("\n")
        index = index + 1
f.close()

# export to Elastic Search
if os.path.exists(XPUT_FILE):
    os.remove(XPUT_FILE)
for i in range(len(yelp_es_csv)):
    initial = "curl -XPUT %s/%s/%s/%d -d '" % (
        AWS_ES_END_POINT, AWS_ES_INDICE, AWS_ES_MAP, i + 1)
    middle = '{"%s": "%s", "%s": "%s"}' % (
        AWS_ES_KEY, yelp_es_csv[AWS_ES_KEY][i],
        AWS_ES_VAL, yelp_es_csv[AWS_ES_VAL][i])
    final = "' -H 'Content-Type: application/json'"
    full = initial + middle + final
    writeXPUT(full)
    # os.system(full)

'''
curl -XPUT https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/Restaurant/1 -d '{"RestaurantID": "RestaurantID", "Cuisine": "Cuisine"}' -H 'Content-Type: application/json'
curl -XGET 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/_search?q=chinese'
'''

import pandas as pd
import os

# constants #
# Yelp CSV config
YELP_CSV = 'Yelp_Restaurants.csv'
YELP_ES_CSV = 'Yelp_Restaurants_Elastic_Search.csv'
# AWS config
AWS_ES_END_POINT = 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com'
AWS_ES_INDEX = 'restaurants'
AWS_ES_ENTRY = 'Restaurant'
AWS_ES_KEY = 'RestaurantID'
AWS_ES_VAL = 'Cuisine'
XPUT_FILE = 'Yelp_Restaurants_Elastic_Search_XPUT.txt'


def removeExists(file):
    if os.path.exists(file):
        os.remove(file)


def createEScsv():
    # create csv for elastic search
    removeExists(YELP_ES_CSV)
    yelp_csv = pd.read_csv(YELP_CSV)
    yelp_es_csv = pd.DataFrame()
    yelp_es_csv = (yelp_csv.loc[:, [AWS_ES_KEY, AWS_ES_VAL]])
    yelp_es_csv.to_csv(path_or_buf=YELP_ES_CSV, index=False)
    yelp_es_csv = pd.read_csv(YELP_ES_CSV)


def writeXPUT():
    # export to Elastic Search
    removeExists(XPUT_FILE)
    yelp_es_csv = pd.read_csv(YELP_ES_CSV)
    for i in range(len(yelp_es_csv)):
        initial = "curl -XPUT %s/%s/%s/%d -d '" % (
            AWS_ES_END_POINT, AWS_ES_INDEX, AWS_ES_ENTRY, i + 1)
        middle = '{"%s": "%s", "%s": "%s"}' % (
            AWS_ES_KEY, yelp_es_csv[AWS_ES_KEY][i],
            AWS_ES_VAL, yelp_es_csv[AWS_ES_VAL][i])
        final = "' -H 'Content-Type: application/json'"
        full = initial + middle + final
        with open(XPUT_FILE, 'a+') as f:
            f.write(full)
            f.write('\n')
        f.close


def uploadToES():
    for line in open(XPUT_FILE):
        # print(line)
        os.system(line)


# process #
createEScsv()
writeXPUT()
uploadToES()

'''
curl -XPUT https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/Restaurant/1 -d '{"RestaurantID": "RestaurantID", "Cuisine": "Cuisine"}' -H 'Content-Type: application/json'
curl -XGET 'https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/_search?q=chinese'
curl -X DELETE "https://search-yelp-restaurants-catcjjrqnh7ynnm3rc7inpu7ky.us-east-1.es.amazonaws.com/restaurants/"  
'''

import pandas as pd
import ipdb
import json
import os

YELP_CSV = 'Yelp_Restaurants.csv'
YELP_CSV_ES = 'Yelp_Restaurants_ES.csv'
YELP_JSON = 'Yelp_Elastic_Search.json'

yelp_csv = pd.read_csv(YELP_CSV)
yelp_es = []

# get csv for elastic search
es_csv = pd.DataFrame()
es_csv = (yelp_csv.loc[:, ['RestaurantID', 'Cuisine']])
es_csv.to_csv(path_or_buf=YELP_CSV_ES, index=False)

# get json for elastic search
for i in range(len(yelp_csv)):
    temp = {}
    temp['Restaurant'] = {}
    temp['Restaurant']['RestaurantID'] = yelp_csv['RestaurantID'][i]
    temp['Restaurant']['Cuisine'] = yelp_csv['Cuisine'][i]
    yelp_es.append(temp)

index = 0
with open(YELP_JSON, 'w+') as f:
    # json.dump(yelp_es, f)
    for row in yelp_es:
        f.write("{ \"index\" : { \"_index\": \"restaurants\", \"_type\" : \"Restaurant\", \"_id\" : \"")
        f.write(str(index))
        f.write("\" } }\n")
        json.dump(yelp_es[index], f)
        f.write("\n")
        index = index + 1
f.close()

'''
index = 0
with open('yelp_restaurants_raw.json', 'w+') as fout:
#    json.dump(new, fout)
    for each in new:
        fout.write("{ \"index\" : { \"_index\": \"restaurants\", \"_type\" : \"Restaurant\", \"_id\" : \"")
        fout.write(str(index))
        fout.write("\" } }\n")
        json.dump(new[index],fout)
        fout.write("\n")
        index = index + 1
fout.close()
'''
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

# es_CSV = pd
es_csv = pd.read_csv(YELP_CSV_ES)

for i in range(len(es_csv)):
    endpoint = 'https://search-yelp-restaurant-yoxrzjvlinry2fzxmbmzijdvqe.us-east-1.es.amazonaws.com/restaurants/Restaurant/' + str(i)
    initial = "curl -XPUT https://search-yelp-restaurant-yoxrzjvlinry2fzxmbmzijdvqe.us-east-1.es.amazonaws.com/restaurants/Restaurant/{} -d ".format(i)
    middle = "'" + "{" + '"Business_ID": "{}", "Cuisine": "{}"'.format(
            es_csv['RestaurantID'][i], es_csv['Cuisine'][i]) + "}" + "' "
    final = "-H " + "'" + "Content-Type: application/json" + "'"
    full = initial + middle + final
    os.system(full)

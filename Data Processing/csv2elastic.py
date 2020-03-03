import pandas
import ipdb
import json

old = pandas.read_csv('yelp_restaurants_raw.csv')

new = []
for i in range(len(old)):
    r = {}
    r['Restaurant'] = {}
    r['Restaurant']['Business_ID'] = old['Business_ID'][i]
    r['Restaurant']['Cuisine'] = old['Cuisine'][i]
    new.append(r)

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

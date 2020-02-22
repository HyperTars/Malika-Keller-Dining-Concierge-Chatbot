import pandas
import ipdb
import json

old = pandas.read_csv('elastic_yelp.csv')

new = []
for i in range(len(old)):
	r = {}
	r['Restaurant'] = {}
	r['Restaurant']['Business_ID'] = old['Business_ID'][i]                                                     
	r['Restaurant']['Cuisine'] = old['Cuisine'][i]  
	new.append(r)

with open('elastic_yelp.json', 'w') as fout:
    json.dump(new, fout)
import pandas
import ipdb
import json
import os


old = pandas.read_csv('elastic_yelp.csv')

new = []
for i in range(len(old)):
	endpoint = 'https://search-yelp-search-7ha4p5bd7kh65doy2drms2kupu.us-east-1.es.amazonaws.com/restaurants/Restaurant/' + str(i)
	initial = "curl -XPUT https://search-yelp-search-7ha4p5bd7kh65doy2drms2kupu.us-east-1.es.amazonaws.com/restaurants/Restaurant/{} -d ".format(i)
	middle =  "'"+ "{" + '"Business_ID": "{}", "Cuisine": "{}"'.format(old['Business_ID'][i],old['Cuisine'][i] ) + "}"+"' "
	final =  "-H " + "'" + "Content-Type: application/json" + "'" 
	full = initial + middle + final
	os.system(full)
	#print(full)

#curl -XPUT https://search-yelp-search-7ha4p5bd7kh65doy2drms2kupu.us-east-1.es.amazonaws.com/restaurants/Restaurant/2 -d '{"Business_ID": "ykXZyQBXxoOMoZfaZMnHmg", "Cuisine": "american"}' -H 'Content-Type: application/json'

#curl -XGET 'https://search-yelp-search-7ha4p5bd7kh65doy2drms2kupu.us-east-1.es.amazonaws.com/restaurants/_search?q=chinese'
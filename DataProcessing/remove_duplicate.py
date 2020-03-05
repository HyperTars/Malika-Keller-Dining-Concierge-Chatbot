import pandas as pd

csv = pd.read_csv('Yelp_Restaurants.csv')
csv.drop_duplicates(subset=['RestaurantID'], keep='first', inplace=True)
csv.to_csv(path_or_buf='Yelp_Restaurants_Remove_Duplicates.csv', index=False)
import requests
import os
from dotenv import load_dotenv

url = "https://api.themoviedb.org/3/movie/popular?language=en-US&page=1"

load_dotenv('C:/Users/Soraya/Desktop/Tommy/CSE_110/movie_project/Scripts/variables.env')

access_token = os.getenv('ACCESS_TOKEN')

headers = {
    "accept": "application/json",
    "Authorization": access_token
}

response = requests.get(url, headers=headers)
data = response.json()

print(data['results'][0]['title'])
print(data['results'][0]['id'])
print(data['results'][0]['release_date'])
print(data['results'][0]['original_language'])
print(data['results'][0]['vote_average'])
print(data['results'][0]['vote_count'])
print(data['results'][0]['popularity'])
print(data['results'][0]['overview'])
print(data['results'][0]['genre_ids'])

# #print(data['results'][1]['title'])
# index = len(data['results'])

# for i in range(0, index):
#     print(data['results'][i]['original_title'])
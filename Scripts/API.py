import requests
import pyodbc
import os
from dotenv import load_dotenv

def calculate_popularity(popularity, max_popularity):
    
    if 0 <= popularity <= max_popularity / 5:
        
        return 'BAJA'
    
    elif popularity <= max_popularity / 1.5:
        
        return 'MEDIA'
    
    else:
        
        return 'ALTA'


load_dotenv('C:/Users/Soraya/Desktop/Tommy/CSE_110/movie_project/Scripts/variables.env')


access_token = os.getenv('ACCESS_TOKEN')
driver = os.getenv('DRIVER')
uid = os.getenv('DB_USER')
pwd = os.getenv('DB_PWD')


headers = {
    "accept": "application/json",
    "Authorization": access_token
}

for page in range(1, 5):

    url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={page}"

    response = requests.get(url, headers=headers)
    data = response.json()

    if page == 1:
        
        most_popular = data['results'][0]['popularity']

    conn = pyodbc.connect(f'DRIVER={driver};SERVER=.\SQLEXPRESS;DATABASE=movie_db;UID={uid};PWD={pwd}')

    cursor = conn.cursor()

    # query1 = f"""
    # CREATE TABLE #Top_Movies (title varchar(50))
    # INSERT INTO #Top_Movies VALUES({i})
    # """


    query2 = """
    IF NOT EXISTS(SELECT 1 FROM Movies WHERE title = ?)
    BEGIN
        INSERT INTO Movies(title, release_date, original_language, vote_average, vote_count, popularity, overview, genreIDs)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)

        INSERT INTO Movie_Popularity(movieID, title, popularity_category)
        VALUES(@@IDENTITY, ?, ?)
    END
    ELSE
    BEGIN
        IF (SELECT popularity FROM Movies WHERE title = ?) != ?
        BEGIN
            UPDATE Movies
            SET title = ?, release_date = ?, original_language = ?, vote_average = ?, vote_count = ?, popularity = ?, overview = ?, genreIDs = ?
            
            UPDATE Movie_Popularity
            SET title = ?, popularity_category = ?
        END
    END
    """

    index = len(data['results'])

    for i in range(0, index):
        
        
        cursor.execute(query2, data['results'][i]['title'],
                    data['results'][i]['title'], data['results'][i]['release_date'], data['results'][i]['original_language'], data['results'][i]['vote_average'], data['results'][i]['vote_count'], data['results'][i]['popularity'], data['results'][i]['overview'], str(data['results'][i]['genre_ids']),
                    data['results'][i]['title'], calculate_popularity(data['results'][i]['popularity'], most_popular),
                    data['results'][i]['title'], data['results'][i]['popularity'], data['results'][i]['title'], data['results'][i]['release_date'], data['results'][i]['original_language'], data['results'][i]['vote_average'], data['results'][i]['vote_count'], data['results'][i]['popularity'], data['results'][i]['overview'], str(data['results'][i]['genre_ids']), data['results'][i]['title'], calculate_popularity(data['results'][i]['popularity'], most_popular))
        
        print(f"Checking data for {data['results'][i]['title']}...")

    conn.commit()
    conn.close()

    print(f"Page {page} finished")

# print(top_movies_list)

# print(data['results'][0]['title'])
# print(data['results'][0]['id'])
# print(data['results'][0]['release_date'])
# print(data['results'][0]['original_language'])
# print(data['results'][0]['vote_average'])
# print(data['results'][0]['vote_count'])
# print(data['results'][0]['popularity'])
# print(data['results'][0]['overview'])
# print(data['results'][0]['genre_ids'])

# #print(data['results'][1]['title'])
# index = len(data['results'])

# for i in range(0, index):
#     print(data['results'][i]['title'])
#     print(data['results'][i]['popularity'])
#     print(data['results'][i]['vote_average'])
#     print()
import requests
import pyodbc
import os
import pandas as pd
from dotenv import load_dotenv

#función para importar los contenidos de la tabla en un CSV
#esta función está reciclada del proyecto anterior
def import_as_csv (driver, server, database, table, user, password, output_file):
    
    try:
    
        print(f'Establishing connection with {server}...')
        
        sql_conn = pyodbc.connect(f'DRIVER={{{driver}}}; SERVER={server};DATABASE={database};UID={user};PWD={password}')
        
        print('Connection successfully established')
        
        print(f'Retrieving data from {table}...')
        
        #se toman todos los datos de la tabla en la base de datos
        query = f"SELECT * FROM {table}"
        
        #se ejecuta la consulta y los datos se convierten en dataframe
        df = pd.read_sql(query, con=sql_conn)
        
        print('Importing as CSV...')
        
        #se convierte el dataframe en archivo CSV y se guarda en la
        #ruta y formato especificados
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print('File imported succesfully!')
        
    except Exception as e:
        print(f'Error: {e}')


    finally:
        #se cierra la conexión
        sql_conn.close()

#función para decifrar los generos de las películas
def decipher_genre(movie_genres, genres_data, array):
    
    #bucle que itera sobre cada uno de los id de genero
    for i in movie_genres:
        
        #bucle que itera sobre todos los generos existentes buscando una coincidencia
        for genre in genres_data['genres']:
            
            if genre['id'] == i:
                
                #Si existe una coincidencia, se añade dicho género a una lista
                array.append(genre['name'])
            
            else:
                
                pass

#función para calcular la popularidad basada en la película más popular
def calculate_popularity(popularity, max_popularity):
    
    if 0 <= popularity <= max_popularity / 5:
        
        return 'BAJA'
    
    elif popularity <= max_popularity / 1.5:
        
        return 'MEDIA'
    
    else:
        
        return 'ALTA'

#cargando variables de entorno
load_dotenv('C:/Users/Soraya/Desktop/Tommy/CSE_110/movie_project/Scripts/variables.env')


access_token = os.getenv('ACCESS_TOKEN')
driver = os.getenv('DRIVER')
uid = os.getenv('DB_USER')
pwd = os.getenv('DB_PWD')
csv_path = os.getenv('FILE_PATH')

#token de acceso a la API
headers = {
    "accept": "application/json",
    "Authorization": access_token
}

#API para extraer los datos sobre los generos de las películas
url_genres = 'https://api.themoviedb.org/3/genre/movie/list?language=en'

#Se accede a los datos y se convierten en un diccionario de Python
response_genres = requests.get(url_genres, headers=headers)
data_genres = response_genres.json()

#bucle que itera sobre las 5 páginas de contenido de la API
for page in range(1, 6):

    #API para extraer datos sobre la popularidad de las películas
    url_movies = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={page}"

    #se extraen los datos y se convierten en un diccionario de Python
    response_movies = requests.get(url_movies, headers=headers)
    data_movies = response_movies.json()

    #se guarda en una variable la popularidad de la primer película de todas
    #(la más popular) para usarla como criterio de popularidad para las demás
    if page == 1:
        
        most_popular = data_movies['results'][0]['popularity']

    #Se establece la conexión con el servidor
    conn = pyodbc.connect(f'DRIVER={driver};SERVER=.\SQLEXPRESS;DATABASE=movie_db;UID={uid};PWD={pwd}')

    #Se crea un cursor para ejecutar consultas SQL
    cursor = conn.cursor()

    #La consulta que se va a realizar (Esta es la parte que falla)
    query2 = """
    IF NOT EXISTS(SELECT 1 FROM Movies WHERE title = ?)
    BEGIN
        INSERT INTO Movies(title, release_date, original_language, vote_average, vote_count, popularity, overview, genres)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)

        INSERT INTO Movie_Popularity(movieID, title, popularity_category)
        VALUES(@@IDENTITY, ?, ?)
    END
    ELSE
    BEGIN
        MERGE Updated_Movie_Data AS TARGET
        USING Movies AS SOURCE
    
            ON (TARGET.movieID = SOURCE.movieID)
        WHEN MATCHED
            AND TARGET.title <> SOURCE.title
            OR TARGET.vote_average <> SOURCE.vote_average
            OR TARGET.vote_count <> SOURCE.vote_count
            OR TARGET.popularity <> SOURCE.popularity
        THEN
            UPDATE
                SET TARGET.title = SOURCE.title
                SET TARGET.release_date = SOURCE.release_date
                SET TARGET.original_language = SOURCE.original_language
                SET TARGET.vote_average = SOURCE.vote_average
                SET TARGET.vote_count = SOURCE.vote_count
                SET TARGET.popularity = SOURCE.popularity
                SET TARGET.overview = SOURCE.overview
                SET TARGET.genres = SOURCE.genres
        
        WHEN NOT MATCHED BY TARGET
            THEN 
                INSERT(title, release_date, original_language, vote_average, vote_count, popularity, overview, genres)
                VALUES(SOURCE.title, SOURCE.release_date, SOURCE.original_language, SOURCE.vote_average, SOURCE.vote_count, SOURCE.popularity, SOURCE.overview, SOURCE.genres)
    
        WHEN NOT MATCHED BY SOURCE
            THEN DELETE;
    END
    """

    #Se crea un índice para iterar sobre cada una de las películas de cada página
    index = len(data_movies['results'])

    #Un bucle que itera sobre el índice recién establecido
    for i in range(0, index):
        
        print(f"Checking data for {data_movies['results'][i]['title']}...")
        
        #Se declara la lista para contener los generos de cada película
        #(por cada iteración se reinicia la lista para anotar los generos
        # de la siguiente película)
        genres = []
        #Se ejecuta la función para decifrar géneros
        decipher_genre(data_movies['results'][i]['genre_ids'], data_genres, genres)
        
        #Se ejecuta la consulta SQL y se insertan los valores apropiados
        cursor.execute(query2, data_movies['results'][i]['title'],
                    data_movies['results'][i]['title'], data_movies['results'][i]['release_date'], data_movies['results'][i]['original_language'], data_movies['results'][i]['vote_average'], data_movies['results'][i]['vote_count'], data_movies['results'][i]['popularity'], data_movies['results'][i]['overview'], str(genres),
                    data_movies['results'][i]['title'], calculate_popularity(data_movies['results'][i]['popularity'], most_popular))
        
        print('Done!')

    #Se guardan los cambios y se cierra al conexión
    conn.commit()
    conn.close()

    print(f"Page {page} finished")
    
#Se ejecuta la función para importar como CSV
import_as_csv(driver, '.\SQLEXPRESS', 'movie_db', 'Movies', uid, pwd, csv_path)
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from indices import genre_index, movies_index, person_index
from models import Film, Genre, Person
from pydantic import BaseModel
from queries import query_film_work, query_genres, query_persons

load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT')),
    'options': '-c search_path=content'
}

ELASTIC_HOST = 'localhost'#os.environ.get('ELASTIC_HOST')
ELASTIC_PORT = 9200
REDIS_HOST = 'localhost'#os.environ.get('REDIS_HOST')
SLEEP_TIME = int(os.environ.get('SLEEP_TIME'))

@dataclass
class ETLConfig:
    query: str
    index_schema: dict
    state_key: str
    elastic_index_name: str
    related_model: BaseModel


ETL_CONFIGS = {
    'movies': ETLConfig(query_film_work, movies_index, 'film_last_modified_date', 'movies', Film),
    'genres': ETLConfig(query_genres, genre_index, 'genre_last_modified_date', 'genres', Genre),
    'persons': ETLConfig(query_persons, person_index, 'person_last_modified_date', 'persons', Person)
}

ETL_INDICES = [
    "movies", "genres", "persons"
]

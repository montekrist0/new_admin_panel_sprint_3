import os
from dataclasses import dataclass
from typing import Callable

from core.indices import genre_index, movies_index, person_index
from core.queries import new_film_query, query_genres, query_persons
from models.models import Film, Genre, Person

dsl = {
    'dbname': os.getenv('DB_NAME', 'movies_database'),
    'user': os.getenv('DB_USER', 'app'),
    'password': os.getenv('DB_PASSWORD', '123qwe'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'options': '-c search_path=content'
}

ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = int(os.getenv('ELASTIC_PORT', 9200))

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

SLEEP_TIME_SECONDS = int(os.getenv('SLEEP_TIME', 60))

DB_FILM_LIMIT = 20000
DB_GENRE_LIMIT = 20000
DB_PERSON_LIMIT = 20000


@dataclass
class ETLConfig:
    """Класс описывающий параметры ETL для отдельной сущности."""
    query: str
    index_schema: dict
    state_key: str
    elastic_index_name: str
    related_model: Callable
    batch_size: int = 100
    limit_size: int = 5000


ETL_CONFIGS = [
    ETLConfig(new_film_query, movies_index, 'film_last_modified_date', 'movies', Film, limit_size=DB_FILM_LIMIT),
    ETLConfig(query_genres, genre_index, 'genre_last_modified_date', 'genres', Genre, limit_size=DB_GENRE_LIMIT),
    ETLConfig(query_persons, person_index, 'person_last_modified_date', 'persons', Person, limit_size=DB_PERSON_LIMIT)
]

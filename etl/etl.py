import elasticsearch.exceptions
import json
import logging
from contextlib import contextmanager
from time import sleep

import backoff
import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as PG_connection
from queries import query_film_work, query_persons, query_genres
from redis import Redis
from settings import dsl, ELASTIC_HOST, REDIS_HOST, SLEEP_TIME, ELASTIC_PORT
from state import RedisStorage, State
from indices import movies_index, person_index, genre_index
from backoff_handlers import (
    pg_conn_backoff_hdlr,
    pg_conn_success_hdlr,
    pg_getdata_backoff_hdlr,
    pg_getdata_success_hdlr,
    elastic_load_data_backoff_hdlr,
    elastic_conn_backoff_hdlr,
)
from dataclasses import dataclass
from pydantic import BaseModel
from models import Film

logging.basicConfig(
    # filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=psycopg2.Error,
    max_tries=10,
    on_backoff=pg_conn_backoff_hdlr,
    on_success=pg_conn_success_hdlr
)
def connect_db(params):
    return psycopg2.connect(
        **params,
        cursor_factory=DictCursor
    )


@contextmanager
def pg_context(params: dict):
    """Connection to db PostgreSQL."""
    conn = connect_db(params)
    # cursor = conn.cursor()
    yield conn
    # cursor.close()
    conn.close()


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=psycopg2.Error,
    on_backoff=pg_getdata_backoff_hdlr,
    on_success=pg_getdata_success_hdlr,
    max_tries=10
)
def get_data_from_pg(
        cursor: DictCursor,
        query: str,
        last_md_date: str = '1970-01-01',
        batch_size: int = 100
) -> list:
    """
    Returns list of rows in dictionary format from database.
    """
    cursor.execute(query.format(
        last_md_date=last_md_date, batch_size=batch_size))
    return [dict(row) for row in cursor.fetchall()]


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=Exception,
    on_backoff=elastic_load_data_backoff_hdlr,
    max_tries=10
)
def load_data_to_elastic(elastic_client: Elasticsearch,
                         transformed_data: list):
    """Loads list of records in Elasticsearch"""
    helpers.bulk(elastic_client, transformed_data)


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=Exception,
    on_backoff=elastic_conn_backoff_hdlr,
    max_tries=10
)
def create_elastic():
    with open("index_schema.json", encoding="utf-8") as file:
        mapping = json.load(file)
    es = Elasticsearch(ELASTIC_HOST)
    try:
        es.indices.create(index="movies", **mapping)
    except Exception as exc:
        logging.info(f"Index insertion error -> {exc}")
    if not es.ping():
        raise Exception("Elastic server is not available")
    return es


@backoff.on_exception(
    wait_gen=backoff.expo,
    exception=Exception,
    on_backoff=elastic_conn_backoff_hdlr,
    max_tries=10
)
def create_elastic_connection():
    es = Elasticsearch(f'http://{ELASTIC_HOST}:{ELASTIC_PORT}')
    if not es.ping():
        raise Exception("Elastic server is not available")
    return es


def create_elastic_index(elastic: Elasticsearch, index: dict) -> None:
    try:
        elastic.indices.create(index="movies", **index)
    except Exception as exc:
        logging.info(f"Index insertion error -> {exc}")


def transform_data(rows: list, index_name):
    """Transform data for uploading to Elasticsearch."""
    return [
        {
            "_index": "movies",
            "_id": row["id"],
            "_source": row
        } for row in rows
    ]


def get_last_modified_date(state: State, key):
    last_modified_date = state.get_state('last_modified_date')
    return last_modified_date if last_modified_date else '1970-01-01'


@dataclass
class PostgresExtractor:
    db_cursor: DictCursor
    query: str
    state_adapter: State
    batch_size: int = 100
    state_key: str = 'last_modified_date'

    def __post_init__(self):
        self.last_modified_date = self.state_adapter.get_state(self.state_key)
        self.last_modified_date = (
            self.last_modified_date if self.last_modified_date
            else '1970-01-01')

    def extract_batch_from_database(self):
        self.db_cursor.execute(self.query.format(
            last_md_date=self.last_modified_date, batch_size=self.batch_size))

        if self.db_cursor.rowcount:
            batch = self.db_cursor.fetchall()
            self.update_state(
                old_value=self.last_modified_date,
                new_value=batch[-1]['modified'].isoformat()
            )
            return batch

    def update_state(self, old_value, new_value):
        self.state_adapter.set_state(self.state_key, new_value)
        logging.info(f'State "{self.state_key}" updated from {old_value} to {new_value}')


@dataclass
class ETLConfig:
    query: str
    index_schema: dict
    state_key: str
    elastic_index_name: str


ETL_DATA = {
    'movies': ETLConfig(query_film_work, movies_index, 'film_last_modified_date', 'movies'),
    'genres': ETLConfig(query_genres, genre_index, 'genre_last_modified_date', 'genres'),
    'persons': ETLConfig(query_persons, person_index, 'person_last_modified_date', 'persons')
}

ETL_DATA_NAMES = [
    "movies", "genres", "persons"
]


def get_extractors(db_conn: PG_connection, state: State):
    return [
        PostgresExtractor(
            db_cursor=db_conn.cursor(),
            query=ETL_DATA[key].query,
            state_adapter=state,
            state_key=ETL_DATA[key].state_key
        ) for key in ETL_DATA_NAMES
    ]


@dataclass
class ElasticsearchLoader:
    index_schema: dict
    index_name: str

    def create_index(self, elastic_conn: Elasticsearch):
        try:
            elastic_conn.indices.create(index=self.index_name, **self.index_schema)
        except Exception as exc:
            logging.info(f'Index {self.index_name} insertion error -> {exc}')
            # возможно стоит выкинуть исключение, чтобы обработать его сверху
            # raise elasticsearch.exceptions.Any

    @staticmethod
    def load_data_to_elastic(elastic_conn: Elasticsearch, transformed_data: list):
        """Loads list of records in Elasticsearch"""
        helpers.bulk(elastic_conn, transformed_data)


def get_loaders():
    return [
        ElasticsearchLoader(
            index_schema=ETL_DATA[key].index_schema,
            index_name=key
        ) for key in ETL_DATA_NAMES
    ]


def main():
    """Main process"""
    logging.info('Start etl process')
    state = State(RedisStorage(Redis(host=REDIS_HOST)))
    elastic = create_elastic_connection()

    with pg_context(dsl) as pg_conn:
        movies_extractor, genre_extractor, person_extractor = get_extractors(pg_conn, state)
        movies_loader, genre_loader, person_loader = get_loaders()

        while True:
            data = movies_extractor.extract_batch_from_database()

            try:
                tr_data = [Film(**row) for row in data]
                a = 1
            except:
                logging.error("Film parsing went wrong")

            if data:
                transformed_data = transform_data(data)
                load_data_to_elastic(elastic_client=elastic,
                                     transformed_data=transformed_data)

            sleep(SLEEP_TIME)


if __name__ == '__main__':
    main()

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep

import backoff
import elasticsearch.exceptions
import psycopg2
from backoff_handlers import (elastic_conn_backoff_hdlr,
                              elastic_load_data_backoff_hdlr,
                              pg_conn_backoff_hdlr, pg_conn_success_hdlr,
                              pg_getdata_backoff_hdlr, pg_getdata_success_hdlr)
from elasticsearch import Elasticsearch, helpers
from indices import genre_index, movies_index, person_index
from models import Film, Genre, PersonBase, Person
from psycopg2.extensions import connection as PG_connection
from psycopg2.extras import DictCursor
from pydantic import BaseModel
from queries import query_film_work, query_genres, query_persons
from redis import Redis
from settings import ELASTIC_HOST, ELASTIC_PORT, REDIS_HOST, SLEEP_TIME, dsl
from state import RedisStorage, State
from typing import List, Union

logging.basicConfig(
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


def get_last_modified_date(state: State, key):
    last_modified_date = state.get_state('last_modified_date')
    return last_modified_date if last_modified_date else '1970-01-01'


@dataclass
class PostgresExtractor:
    db_cursor: DictCursor
    batch_size: int = 100

    def __post_init__(self):
        self.last_modified_date = self.state_adapter.get_state(self.state_key)
        self.last_modified_date = (
            self.last_modified_date if self.last_modified_date
            else '1970-01-01')

    def extract_batch_from_database(self, query):
        self.db_cursor.execute(query.format(
            last_md_date=self.last_modified_date, batch_size=self.batch_size))
        rows_count = self.db_cursor.rowcount
        if rows_count:
            batch = self.db_cursor.fetchall()
            self.update_state(
                old_value=self.last_modified_date,
                new_value=batch[-1]['modified'].isoformat()
            )
            logging.info(f'\tExtracted {rows_count} rows for {self}s')
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
    related_model: Union[Film, Genre, Person]


ETL_CONFIGS = [
    ETLConfig(query_film_work, movies_index, 'film_last_modified_date', 'movies', Film),
    ETLConfig(query_genres, genre_index, 'genre_last_modified_date', 'genres', Genre),
    ETLConfig(query_persons, person_index, 'person_last_modified_date', 'persons', Person)
]

ETL_INDICES = [
    "movies", "genres", "persons"
]


def get_extractors(db_conn: PG_connection, state: State):
    return [
        PostgresExtractor(db_cursor=db_conn.cursor(), state_adapter=state) for _ in ETL_INDICES
    ]


class ElasticsearchLoader:
    @staticmethod
    def create_index(index_name: str, index_schema: dict, elastic_conn: Elasticsearch):
        try:
            elastic_conn.indices.create(index=index_name, **index_schema)
        except Exception as exc:
            logging.info(f'Index {index_name} insertion error -> {exc}')

    @staticmethod
    def load_data_to_elastic(elastic_conn: Elasticsearch, transformed_data: list):
        """Loads list of records in Elasticsearch"""
        helpers.bulk(elastic_conn, transformed_data)


def get_loaders():
    return [
        ElasticsearchLoader() for _ in ETL_INDICES
    ]


@dataclass
class ETLHandler:
    extractor: PostgresExtractor
    loader: ElasticsearchLoader
    config: ETLConfig

    def get_last_modified_date(self):
        self.last_modified_date = self.state_adapter.get_state(self.state_key)
        self.last_modified_date = (
            self.last_modified_date if self.last_modified_date
            else '1970-01-01')

    def transform_data(self, rows: list):
        """Transform data for uploading to Elasticsearch."""
        try:
            return [
                {
                    "_index": self.loader.index_name,
                    "_id": row['id'],
                    "_source": self.extractor.related_model(**row).json()
                } for row in rows
            ]
        except Exception as exc:
            logging.info(exc)
            raise exc

    def process(self, elastic_conn):
        data = self.extractor.extract_batch_from_database()
        if data:
            transformed_data = self.transform_data(rows=data)
            self.loader.load_data_to_elastic(elastic_conn, transformed_data)


def get_etl_handlers(indices: List[ETLConfig], pg_conn: PG_connection, state: State, elastic: Elasticsearch):
    return [
        ETLHandler(extractor, loader, config)
        for extractor, loader, config in zip(
            get_extractors(pg_conn, state),
            get_loaders(),
            indices
        )
    ]


def main():
    """Main process"""
    logging.info('Start etl process')

    state = State(RedisStorage(Redis(host=REDIS_HOST)))
    elastic = create_elastic_connection()
    with pg_context(dsl) as pg_conn:
        etl_handlers = get_etl_handlers(ETL_INDICES, pg_conn, state, elastic)
        etl_handlers = [
            ETLHandler(extractor, loader) for extractor, loader in zip(get_extractors(pg_conn, state), get_loaders())
        ]

        for etl_handler in etl_handlers:
            etl_handler.loader.create_index(elastic)

        while True:
            for etl_handler in etl_handlers:
                etl_handler.process(elastic)


if __name__ == '__main__':
    main()

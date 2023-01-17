import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep

import backoff
import psycopg2
from backoff_handlers import (elastic_conn_backoff_hdlr,
                              elastic_load_data_backoff_hdlr,
                              pg_conn_backoff_hdlr, pg_conn_success_hdlr,
                              pg_getdata_backoff_hdlr, pg_getdata_success_hdlr)
from elasticsearch import Elasticsearch, helpers
from indices import genre_index, movies_index, person_index
from models import Film, Genre, Person
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
    exception=Exception,
    on_backoff=elastic_conn_backoff_hdlr,
    max_tries=10
)
def create_elastic_connection():
    es = Elasticsearch(f'http://{ELASTIC_HOST}:{ELASTIC_PORT}')
    if not es.ping():
        raise Exception("Elastic server is not available")
    return es


@dataclass
class PostgresExtractor:
    db_cursor: DictCursor

    def extract_batch_from_database(self, query: str):
        self.db_cursor.execute(query)
        rows_count = self.db_cursor.rowcount
        if rows_count:
            batch = self.db_cursor.fetchall()
            return batch


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


@dataclass
class ETLHandler:
    extractor: PostgresExtractor
    loader: ElasticsearchLoader
    config: ETLConfig
    batch_size: int = 100
    state_option: str = 'modified'

    def transform_data(self, rows: list):
        """Transform data for uploading to Elasticsearch."""
        try:
            return [
                {
                    "_index": self.config.elastic_index_name,
                    "_id": row['id'],
                    "_source": self.config.related_model(**row).json()
                } for row in rows
            ]
        except Exception as exc:
            logging.info(exc)
            raise exc

    def process(self, elastic_conn, state: State):
        self.last_modified_date = state.get_state(key=self.config.state_key, default='1970-01-01')
        formatted_query = self.config.query.format(
            last_md_date=self.last_modified_date, batch_size=self.batch_size)
        data = self.extractor.extract_batch_from_database(query=formatted_query)
        if data:
            new_last_modified_date = data[-1][self.state_option].isoformat()
            transformed_data = self.transform_data(rows=data)
            try:
                self.loader.load_data_to_elastic(elastic_conn, transformed_data)
            except Exception as exc:
                logging.error(exc)
            else:
                state.set_state(self.config.state_key, new_last_modified_date)
                logging.info(f'\tExtracted {len(data)} rows for {self.config.elastic_index_name}')
                logging.info(
                    f'State "{self.config.state_key}" updated from {self.last_modified_date} to {new_last_modified_date}')


def main():
    """Main process"""
    logging.info('Start etl process')

    state = State(RedisStorage(Redis(host=REDIS_HOST)))
    # для отладки
    state.storage.clear_cache()
    # ----
    elastic = create_elastic_connection()
    with pg_context(dsl) as pg_conn:
        etl_handlers = [
            ETLHandler(PostgresExtractor(pg_conn.cursor()), ElasticsearchLoader, config) for config in ETL_CONFIGS
        ]

        for etl_handler in etl_handlers:
            etl_handler.loader.create_index(
                index_name=etl_handler.config.elastic_index_name,
                index_schema=etl_handler.config.index_schema,
                elastic_conn=elastic)

        while True:
            for etl_handler in etl_handlers:
                etl_handler.process(elastic_conn=elastic, state=state)


if __name__ == '__main__':
    main()

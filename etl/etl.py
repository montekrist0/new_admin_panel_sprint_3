import json
import logging
from contextlib import contextmanager
from time import sleep

import backoff
import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictCursor
from queries import query_film_work
from redis import Redis
from settings import dsl, ELASTIC_HOST, REDIS_HOST, SLEEP_TIME
from state import RedisStorage, State

logging.basicConfig(
    # filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)


def pg_conn_backoff_hdlr(details):
    logging.info("\t\n ==> Backing off {wait:0.1f} seconds after {tries} tries "
                 "connection to PostgreSQL".format(**details))


def pg_conn_success_hdlr(details):
    logging.info("==> Successfully connected to PostgreSQL")


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
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()


def pg_getdata_backoff_hdlr(details):
    logging.info(
        "\t\n ==> Can't execute query PostgreSQL."
        "Backing off {wait:0.1f} seconds after {tries} tries"
        "Details: {args}".format(**details))


def pg_getdata_success_hdlr(details):
    logging.info("==> Query executed successfully to PostgreSQL.")


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


def elastic_load_data_backoff_hdlr(details):
    logging.info(
        "\t\n ==> Can't load data to Elastic query PostgreSQL. "
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "Details: {args}".format(**details))


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


def elastic_conn_backoff_hdlr(details):
    logging.info(
        "\t\n ==> Elastic connection Error. "
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "Details: {args}".format(**details))


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


def transform_data(rows: list):
    """Transform data for uploading to Elasticsearch."""
    for row in rows:
        del row["modified"]

    return [
        {
            "_index": "movies",
            "_id": row["id"],
            "_source": row
        } for row in rows
    ]


def main():
    """Main process"""
    logging.info('Start etl process')
    state = State(RedisStorage(Redis(host=REDIS_HOST)))
    elastic = create_elastic()
    with pg_context(dsl) as pg_cursor:
        while True:
            last_modified_date = state.get_state('last_modified_date')
            last_modified_date = (
                last_modified_date if last_modified_date
                else '1970-01-01')

            data = get_data_from_pg(cursor=pg_cursor,
                                    query=query_film_work,
                                    last_md_date=last_modified_date,
                                    batch_size=100)

            if data:
                last_modified_date = data[-1]['modified'].isoformat()
                transformed_data = transform_data(data)
                load_data_to_elastic(elastic_client=elastic,
                                     transformed_data=transformed_data)
                state.set_state('last_modified_date', last_modified_date)
            sleep(SLEEP_TIME)


if __name__ == '__main__':
    main()

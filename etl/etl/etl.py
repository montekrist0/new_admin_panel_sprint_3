import asyncio
import logging

import elasticsearch
import psycopg2
from db.connection_handler import (PostgreConnError, connect_db,
                                   create_elastic_connection,
                                   create_redis_connection)
from redis import exceptions as redis_exceptions
from services.elastic_loader_service import create_indices
from services.etl_handler_service import get_etl_handlers
from services.state_service import RedisStorage, State
from settings import ELASTIC_HOST, ELASTIC_PORT, ETL_CONFIGS, REDIS_HOST, dsl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)


async def start_etl():
    """Main process"""
    logging.info('Start etl process')
    db_conn = None
    while True:

        try:
            state = State(RedisStorage(create_redis_connection(REDIS_HOST)))
            elastic = create_elastic_connection(ELASTIC_HOST, ELASTIC_PORT)
            db_conn = connect_db(dsl)
            etl_handlers = get_etl_handlers(db_conn, ETL_CONFIGS)
            create_indices(etl_handlers, elastic)

            while True:

                try:
                    tasks = []
                    for etl_handler in etl_handlers:
                        tasks.append(etl_handler.process(elastic_conn=elastic, state=state))
                    await asyncio.gather(*tasks)
                    logging.info('ETL resume. Start scan for changes')

                except PostgreConnError as db_error:
                    logging.exception(f"Extraction failed. Database connection closed: \n\t {db_error}. "
                                      f"****** Trying to reconnect... ******")
                    db_conn.close()
                    db_conn = connect_db(dsl)
                    for etl_handler in etl_handlers:
                        etl_handler.extractor.db_cursor = db_conn.cursor()

                except elasticsearch.ConnectionError:
                    logging.info("...Reconnect to elastic")
                    elastic = create_elastic_connection(ELASTIC_HOST, ELASTIC_PORT)

                except redis_exceptions.RedisError as redis_exc:
                    logging.exception(f'Redis error occured:\n\t {redis_exc} ')
                    state = State(RedisStorage(create_redis_connection(REDIS_HOST)))

        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.ProgrammingError):
            db_conn.close()

        except Exception as exc:
            logging.exception(f'!!! Something went wrong... \n\t {exc}')


def main():
    asyncio.run(start_etl())


if __name__ == '__main__':
    main()

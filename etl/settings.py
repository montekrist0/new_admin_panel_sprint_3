import os

from dotenv import load_dotenv

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

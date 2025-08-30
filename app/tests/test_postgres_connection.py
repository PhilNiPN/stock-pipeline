from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
from psycopg2 import OperationalError

load_dotenv(find_dotenv())

def test_connection():
    # Read DB config from environment variables
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'db'),
        'port': 5432,  # docker port 5432 
        'dbname': os.getenv('POSTGRES_DB', 'stockdb'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }

    try:
        conn = psycopg2.connect(**db_config)
        print(' PostgreSQL connection successful!')
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()
        print('Database version:', version[0])
        cur.close()
        conn.close()
    except OperationalError as e:
        print('Connection failed:', e)

if __name__ == '__main__':
    test_connection()

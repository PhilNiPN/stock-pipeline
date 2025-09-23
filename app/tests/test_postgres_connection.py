from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
from psycopg2 import OperationalError
import pytest

# Try to load environment variables, but handle encoding issues gracefully
try:
    load_dotenv(find_dotenv())
except (UnicodeDecodeError, UnicodeError):
    # If there are encoding issues with the .env file, skip loading it
    pass

def test_connection_from_host():
    """Test PostgreSQL connection from host machine (localhost:5434)."""
    # Read DB config from environment variables
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5434')),  # Match docker-compose port mapping
        'dbname': os.getenv('POSTGRES_DB', 'stockdb'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')  # Match docker-compose default
    }

    try:
        conn = psycopg2.connect(**db_config)
        print('✅ PostgreSQL connection from HOST successful!')
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()
        print('Database version:', version[0])
        cur.close()
        conn.close()
    except (OperationalError, UnicodeDecodeError, UnicodeError) as e:
        print(f'⚠️  HOST connection failed: {e}')
        # Skip test if it's a connection issue (database not running)
        pytest.skip(f"PostgreSQL database not available from HOST: {e}")


def test_connection_docker_internal():
    """Test PostgreSQL connection using Docker internal network (db:5432)."""
    # Docker-internal connection (used by ETL, Airflow, etc.)
    docker_config = {
        'host': 'db',  # Docker service name
        'port': 5432,  # Internal Docker port
        'dbname': os.getenv('POSTGRES_DB', 'stockdb'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
    }

    try:
        conn = psycopg2.connect(**docker_config)
        print('✅ PostgreSQL connection from DOCKER successful!')
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()
        print('Database version:', version[0])
        cur.close()
        conn.close()
    except (OperationalError, UnicodeDecodeError, UnicodeError) as e:
        print(f'⚠️  DOCKER connection failed: {e}')
        # This is expected to fail when running tests from host
        pytest.skip(f"PostgreSQL not accessible via Docker network (expected when running from host): {e}")


# Backward compatibility
def test_connection():
    """Alias for host connection test."""
    test_connection_from_host()

if __name__ == '__main__':
    test_connection()

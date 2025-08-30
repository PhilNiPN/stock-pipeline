import sys
import os

# Load environment variables from .env file if it exists
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

print('---- Env Var Debugging ----')
print(f'Current working directory: {os.getcwd()}')

# Check if .env file exists
env_file = find_dotenv()
print(f'Looking for .env file at: {env_file}')
if os.path.exists(env_file):
    print(' .env file found')
else:
    print(' .env file not found')

# Access environment variables that are in docker-compose.yml
print('\n ---- Docker Compose Environment Variables ----')
DB_URL = os.getenv('DB_URL')
TICKERS = os.getenv('TICKERS')
START_DATE = os.getenv('START_DATE')
END_DATE = os.getenv('END_DATE')
INTERVAL = os.getenv('INTERVAL')
TABLE_NAME = os.getenv('TABLE_NAME')

print(f'DB_URL: {DB_URL}')
print(f'TICKERS: {TICKERS}')
print(f'START_DATE: {START_DATE}')
print(f'END_DATE: {END_DATE}')
print(f'INTERVAL: {INTERVAL}')
print(f'TABLE_NAME: {TABLE_NAME}')

# PostgreSQL connection variables 
print('\n ---- Postgres connection variables ----')
POST_HOST = os.getenv('POSTGRES_HOST', 'db')  # Default to 'db' (Docker service name)
POST_PORT = os.getenv('POSTGRES_PORT', '5432')
POST_USER = os.getenv('POSTGRES_USER', 'postgres')
POST_DB = os.getenv('POSTGRES_DB', 'stockdb')
POST_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')

print(f'POSTGRES_HOST: {POST_HOST}')
print(f'POSTGRES_PORT: {POST_PORT}')
print(f'POSTGRES_USER: {POST_USER}')
print(f'POSTGRES_DB: {POST_DB}')
print(f'POSTGRES_PASSWORD: {'*' * len(POST_PASSWORD) if POST_PASSWORD else 'None'}')

print('\n ---- All Environment Variables ----')
for key, value in sorted(os.environ.items()):
    if key.startswith(('POSTGRES_', 'DB_', 'TICKERS', 'START_', 'END_', 'INTERVAL', 'TABLE_')):
        print(f'{key}: {value}')

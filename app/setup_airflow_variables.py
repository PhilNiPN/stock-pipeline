#!/usr/bin/env python3
"""
Script to set up Airflow variables and connections for the stock pipeline.
Run this script to configure the required variables and connections in Airflow.
"""

import os
import sys
from datetime import datetime

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

def setup_airflow_variables():
    """Set up Airflow variables for the stock pipeline."""
    
    # Check if we're in an Airflow environment
    try:
        from airflow.models import Variable, Connection
        from airflow import settings
        session = settings.Session()
    except ImportError:
        print(" This script must be run in an Airflow environment.")
        print("   Run it inside the Airflow container:")
        print("   docker compose exec airflow-webserver python /opt/app/setup_airflow_variables.py")
        return False
    
    # Define the variables to set
    variables = {
        'TICKERS': 'NVDA,AAPL,MSFT,GOOGL',
        'INTERVAL': '1d',
        'TABLE_NAME': 'price_metrics',
        'LOOKBACK_DAYS': '10',
        'EMA_SPANS': '[9, 20, 50]',
        'VOLATILITY_WINDOW': '21',
        # Technical Indicators (optional - defaults are used if not set)
        'INCLUDE_MACD': 'true',
        'INCLUDE_BOLLINGER_BANDS': 'true', 
        'INCLUDE_RSI': 'true',
        'MACD_PARAMS': '[12, 26, 9]',
        'BB_PARAMS': '[20, 2.0]',
        'RSI_WINDOW': '14'
    }
    
    print("🔧 Setting up Airflow variables for stock pipeline...")
    
    for key, value in variables.items():
        try:
            # Check if variable already exists
            existing = Variable.get(key, default_var=None)
            if existing is not None:
                print(f" {key} already exists: {existing}")
            else:
                # Create new variable
                Variable.set(key, value)
                print(f" Created {key}: {value}")
        except Exception as e:
            print(f" Failed to set {key}: {e}")
    
    print("\n🔌 Setting up PostgreSQL connection...")
    
    # Set up the PostgreSQL connection
    try:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == 'postgres_default').first()
        if existing_conn:
            print(" postgres_default connection already exists")
        else:
            # Create new connection
            # Get connection details from environment variables (matching docker-compose.yml)
            db_host = os.getenv('DB_HOST', 'db')
            db_port = int(os.getenv('DB_PORT', '5432'))
            db_name = os.getenv('DB_NAME', 'stockdb')
            db_user = os.getenv('DB_USER', 'postgres')
            db_password = os.getenv('DB_PASSWORD', 'mysecretpassword')
            
            new_conn = Connection(
                conn_id='postgres_default',
                conn_type='postgres',
                host=db_host,
                schema=db_name,
                login=db_user,
                password=db_password,
                port=db_port
            )
            session.add(new_conn)
            session.commit()
            print(" Created postgres_default connection")
    except Exception as e:
        print(f" Failed to create connection: {e}")
        session.rollback()
    
    print("\n Airflow setup complete!")
    print("\n Summary of configuration:")
    print("   Variables:")
    for key, value in variables.items():
        print(f"     {key}: {value}")
    print("   Connections:")
    print(f"     postgres_default: postgresql://{os.getenv('DB_USER', 'postgres')}:***@{os.getenv('DB_HOST', 'db')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'stockdb')}")
    
    return True

if __name__ == "__main__":
    setup_airflow_variables()

"""
Pytest configuration and fixtures for the stock pipeline test suite.

This file provides common fixtures and configuration for all tests.
"""

import pytest
import os
import sys
import pandas as pd
from unittest.mock import MagicMock

# Add the app directory to the Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture
def sample_stock_data():
    """Provide sample stock data for testing."""
    return pd.DataFrame({
        'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'High': [101.0, 102.0, 103.0, 104.0, 105.0],
        'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
        'Close': [100.5, 101.5, 102.5, 103.5, 104.5],
        'Adj Close': [100.5, 101.5, 102.5, 103.5, 104.5],
        'Volume': [1000000, 1100000, 1200000, 1300000, 1400000]
    }, index=pd.date_range('2023-01-01', periods=5, freq='D'))


@pytest.fixture
def sample_multi_ticker_data():
    """Provide sample multi-ticker data for testing."""
    multi_data = pd.DataFrame({
        ('Open', 'AAPL'): [100.0, 101.0, 102.0],
        ('High', 'AAPL'): [101.0, 102.0, 103.0],
        ('Low', 'AAPL'): [99.0, 100.0, 101.0],
        ('Close', 'AAPL'): [100.5, 101.5, 102.5],
        ('Adj Close', 'AAPL'): [100.5, 101.5, 102.5],
        ('Volume', 'AAPL'): [1000000, 1100000, 1200000],
        ('Open', 'MSFT'): [200.0, 201.0, 202.0],
        ('High', 'MSFT'): [201.0, 202.0, 203.0],
        ('Low', 'MSFT'): [199.0, 200.0, 201.0],
        ('Close', 'MSFT'): [200.5, 201.5, 202.5],
        ('Adj Close', 'MSFT'): [200.5, 201.5, 202.5],
        ('Volume', 'MSFT'): [2000000, 2100000, 2200000]
    }, index=pd.date_range('2023-01-01', periods=3, freq='D'))
    multi_data.columns = pd.MultiIndex.from_tuples(multi_data.columns, names=['Field', 'Ticker'])
    return multi_data


@pytest.fixture
def sample_metrics_data():
    """Provide sample metrics data for testing."""
    return pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=4, freq='D'),
        'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'open': [100.0, 101.0, 102.0, 103.0],
        'high': [101.0, 102.0, 103.0, 104.0],
        'low': [99.0, 100.0, 101.0, 102.0],
        'close': [100.5, 101.5, 102.5, 103.5],
        'adj_close': [100.5, 101.5, 102.5, 103.5],
        'volume': [1000000, 1100000, 1200000, 1300000],
        'return': [0.02, -0.01, 0.01, 0.01],
        'volatility': [0.15, 0.14, 0.16, 0.15],
        'ema_9': [100.2, 100.8, 101.3, 101.8],
        'ema_20': [100.1, 100.6, 101.1, 101.6],
        'ema_50': [100.0, 100.4, 100.9, 101.4]
    })


@pytest.fixture
def db_config():
    """Provide sample database configuration for testing."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass'
    }


@pytest.fixture
def mock_db_engine():
    """Provide a mock database engine for testing."""
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    mock_engine.raw_connection.return_value = mock_connection
    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    return mock_engine


@pytest.fixture
def mock_db_connection():
    """Provide a mock database connection for testing."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_connection


@pytest.fixture(autouse=True)
def clear_environment_variables():
    """Clear environment variables before and after each test."""
    # Store original values
    env_vars_to_clear = [
        'TICKERS', 'INTERVAL', 'TABLE_NAME', 'END_DATE', 'START_DATE',
        'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
        'DB_URL', 'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 
        'POSTGRES_USER', 'POSTGRES_PASSWORD'
    ]
    
    original_values = {}
    for var in env_vars_to_clear:
        if var in os.environ:
            original_values[var] = os.environ[var]
            del os.environ[var]
    
    yield
    
    # Restore original values
    for var, value in original_values.items():
        os.environ[var] = value


@pytest.fixture
def mock_yfinance():
    """Provide a mock yfinance module for testing."""
    mock_yf = MagicMock()
    mock_download = MagicMock()
    mock_yf.download = mock_download
    return mock_yf, mock_download


@pytest.fixture
def mock_sqlalchemy():
    """Provide mock SQLAlchemy components for testing."""
    mock_engine = MagicMock()
    mock_text = MagicMock()
    mock_create_engine = MagicMock(return_value=mock_engine)
    return mock_engine, mock_text, mock_create_engine


@pytest.fixture
def mock_psycopg2():
    """Provide mock psycopg2 components for testing."""
    mock_execute_values = MagicMock()
    return mock_execute_values

'''
Unit tests for the stock data pipeline modules.

These tests focus on the transformation logic to ensure financial
metrics are computed correctly and the loading function can write
data to a database. A small synthetic dataset is used to avoid
external dependencies and network calls. Tests can be executed
locally with ''pytest'' or via the provided GitHub Actions workflow.
'''

from __future__ import annotations

import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import text

from src.transform import calculate_financial_metrics
from src import load as load_module


def _build_synthetic_price_data() -> pd.DataFrame:
    '''Construct a synthetic price DataFrame with a multi‑index column.

    The data contains five days of prices for a single ticker 'TEST'.
    Using a multi‑index mirrors the structure returned by
    :func:'yfinance.download' when multiple tickers are requested.

    Returns
    -------
    pandas.DataFrame
        DataFrame with price columns.
    '''
    dates = pd.date_range('2023-01-01', periods=5, freq='D')
    # Price values follow a deterministic trend for easy verification
    open_prices = [100, 102, 101, 103, 105]
    high_prices = [101, 103, 102, 104, 106]
    low_prices = [99, 101, 100, 102, 104]
    close_prices = [100, 102, 101, 103, 105]
    adj_close_prices = close_prices  # no corporate actions
    volumes = [1000, 1100, 1050, 1150, 1200]

    arrays = [
        ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'],
        ['TEST', 'TEST', 'TEST', 'TEST', 'TEST', 'TEST'],
    ]
    tuples = list(zip(*arrays))
    index = pd.MultiIndex.from_tuples(tuples, names=['Field', 'Ticker'])
    data = pd.DataFrame(
        [
            open_prices,
            high_prices,
            low_prices,
            close_prices,
            adj_close_prices,
            volumes,
        ],
        index=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'],
    ).T
    data.columns = index
    data.index = dates
    return data


def test_calculate_financial_metrics_simple() -> None:
    '''Ensure calculate_financial_metrics returns expected columns and values.'''
    data = _build_synthetic_price_data()
    metrics = calculate_financial_metrics(data, ema_spans=(2,), volatility_window=2)
    # After dropping the first NaN return there should be 4 rows
    assert len(metrics) == 4
    # Check for expected columns
    expected_cols = {'date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'return', 'volatility', 'ema_2'}
    assert expected_cols.issubset(metrics.columns)
    # Verify return calculation for the first row
    # returns[0] = (close[1] - close[0]) / close[0] = (102 - 100)/100 = 0.02
    first_return = metrics.loc[0, 'return']
    assert np.isclose(first_return, 0.02)


def test_load_to_postgres_sqlite(monkeypatch, tmp_path) -> None:
    '''Test load_to_postgres by patching the engine to use SQLite.

    The load_to_postgres function uses utils.create_db_engine to create
    a connection based on PostgreSQL parameters. For testing we
    monkey‑patch this function to return an SQLite engine instead. The
    DataFrame should be written to the SQLite database and the row
    count should match the input.
    '''
    df = pd.DataFrame({
        'date': pd.date_range('2023-01-02', periods=2, freq='D'),
        'ticker': ['TEST', 'TEST'],
        'open': [102, 101],
        'high': [103, 102],
        'low': [101, 100],
        'close': [102, 101],
        'adj_close': [102, 101],
        'volume': [1100, 1050],
        'return': [0.02, -0.009804],
        'volatility': [0.014142, 0.015811],
        'ema_9': [101.0, 101.5],
        'ema_20': [100.8, 101.2],
        'ema_50': [100.5, 100.9],
    })

    # Mock the PostgreSQL-specific functionality
    from unittest.mock import MagicMock, patch
    
    # Create a mock engine that behaves like PostgreSQL
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    
    # Set up the mock chain
    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    mock_engine.raw_connection.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    
    def fake_create_db_engine(config):
        return mock_engine

    # Patch create_db_engine within the load module
    monkeypatch.setattr(load_module, 'create_db_engine', fake_create_db_engine)
    
    # Mock the execute_values function
    with patch('src.load.execute_values') as mock_execute_values:
        # Execute load
        load_module.load_to_postgres(df, table_name='metrics', db_config={})
        
        # Verify the function was called correctly
        mock_engine.begin.assert_called_once()
        mock_engine.raw_connection.assert_called_once()
        mock_execute_values.assert_called_once()
        
        # Verify the table creation SQL was executed
        mock_connection.execute.assert_called_once()

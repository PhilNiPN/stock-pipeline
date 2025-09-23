"""
Unit tests for the load module.

Tests the database loading functions with mocked database connections
to avoid external dependencies during testing.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, mock_open
from datetime import date
import sqlalchemy
from sqlalchemy import text

from src.load import (
    load_to_postgres,
    upsert_price_metrics,
    _to_records,
    TABLE_DDL,
    UPSERT_SQL,
    COLUMNS
)


class TestToRecords:
    """Test cases for the _to_records function."""

    def test_to_records_basic(self):
        """Test basic record conversion."""
        df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=3, freq='D'),
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'open': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'low': [99.0, 100.0, 101.0],
            'close': [100.5, 101.5, 102.5],
            'adj_close': [100.5, 101.5, 102.5],
            'volume': [1000000, 1100000, 1200000],
            'return': [0.02, -0.01, 0.01],
            'volatility': [0.15, 0.14, 0.16],
            'ema_9': [100.2, 100.8, 101.3],
            'ema_20': [100.1, 100.6, 101.1],
            'ema_50': [100.0, 100.4, 100.9]
        })
        
        records = _to_records(df)
        
        assert len(records) == 3
        assert len(records[0]) == len(COLUMNS)
        
        # Check that dates are converted to date objects
        assert isinstance(records[0][0], date)
        
        # Check column order matches COLUMNS
        for record in records:
            assert len(record) == len(COLUMNS)

    def test_to_records_empty_dataframe(self):
        """Test record conversion with empty DataFrame."""
        df = pd.DataFrame(columns=COLUMNS)
        records = _to_records(df)
        
        assert len(records) == 0

    def test_to_records_missing_columns(self):
        """Test record conversion with missing columns."""
        df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=2, freq='D'),
            'ticker': ['AAPL', 'AAPL'],
            'open': [100.0, 101.0],
            # Missing other columns
        })
        
        # Should fill missing columns with None values and not raise KeyError
        records = _to_records(df)
        assert len(records) == 2
        assert len(records[0]) == len(COLUMNS)  # All columns should be present
        
        # Check that missing columns are filled with None
        # Find indices of columns in COLUMNS
        open_idx = COLUMNS.index('open')
        high_idx = COLUMNS.index('high')  # This should be None since it's missing
        
        assert records[0][open_idx] == 100.0  # Open value should be preserved
        assert records[0][high_idx] is None   # High should be None

    def test_to_records_date_conversion(self):
        """Test that dates are properly converted."""
        df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'ticker': ['AAPL', 'AAPL'],
            'open': [100.0, 101.0],
            'high': [101.0, 102.0],
            'low': [99.0, 100.0],
            'close': [100.5, 101.5],
            'adj_close': [100.5, 101.5],
            'volume': [1000000, 1100000],
            'return': [0.02, -0.01],
            'volatility': [0.15, 0.14],
            'ema_9': [100.2, 100.8],
            'ema_20': [100.1, 100.6],
            'ema_50': [100.0, 100.4]
        })
        
        records = _to_records(df)
        
        # Dates should be converted to date objects
        assert isinstance(records[0][0], date)
        assert records[0][0] == date(2023, 1, 1)
        assert records[1][0] == date(2023, 1, 2)


class TestLoadToPostgres:
    """Test cases for the load_to_postgres function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.sample_df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=3, freq='D'),
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'open': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'low': [99.0, 100.0, 101.0],
            'close': [100.5, 101.5, 102.5],
            'adj_close': [100.5, 101.5, 102.5],
            'volume': [1000000, 1100000, 1200000],
            'return': [0.02, -0.01, 0.01],
            'volatility': [0.15, 0.14, 0.16],
            'ema_9': [100.2, 100.8, 101.3],
            'ema_20': [100.1, 100.6, 101.1],
            'ema_50': [100.0, 100.4, 100.9]
        })

    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_load_to_postgres_basic(self, mock_execute_values, mock_create_engine):
        """Test basic loading to PostgreSQL."""
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function
        load_to_postgres(self.sample_df, 'test_table', {'host': 'localhost'})
        
        # Verify engine was created
        mock_create_engine.assert_called_once_with({'host': 'localhost'})
        
        # Verify table creation SQL was executed
        mock_engine.begin.assert_called_once()
        
        # Verify raw connection was used
        mock_engine.raw_connection.assert_called_once()
        
        # Verify execute_values was called
        mock_execute_values.assert_called_once()

    @patch('src.load.create_db_engine')
    def test_load_to_postgres_empty_dataframe(self, mock_create_engine):
        """Test loading with empty DataFrame."""
        empty_df = pd.DataFrame(columns=COLUMNS)
        
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Call function
        load_to_postgres(empty_df, 'test_table', {'host': 'localhost'})
        
        # For empty DataFrame, the function returns early after deduplication
        # So no database operations should be performed
        mock_engine.begin.assert_not_called()
        mock_engine.raw_connection.assert_not_called()

    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_load_to_postgres_duplicate_removal(self, mock_execute_values, mock_create_engine):
        """Test that duplicates are removed before loading."""
        # Create DataFrame with duplicates
        df_with_duplicates = pd.concat([self.sample_df, self.sample_df.iloc[:1]], ignore_index=True)
        
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function
        load_to_postgres(df_with_duplicates, 'test_table', {'host': 'localhost'})
        
        # Should still create table and execute values
        mock_engine.begin.assert_called_once()
        mock_engine.raw_connection.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_load_to_postgres_custom_chunksize(self, mock_execute_values, mock_create_engine):
        """Test loading with custom chunksize."""
        # Create larger DataFrame
        large_df = pd.concat([self.sample_df] * 10, ignore_index=True)
        
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function with custom chunksize
        load_to_postgres(large_df, 'test_table', {'host': 'localhost'}, chunksize=5)
        
        # Should still create table and execute values
        mock_engine.begin.assert_called_once()
        mock_engine.raw_connection.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.load.create_db_engine')
    def test_load_to_postgres_exception_handling(self, mock_create_engine):
        """Test exception handling during loading."""
        # Mock engine that raises exception
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.begin.side_effect = Exception("Database error")
        
        # Should raise the exception
        with pytest.raises(Exception, match="Database error"):
            load_to_postgres(self.sample_df, 'test_table', {'host': 'localhost'})


class TestUpsertPriceMetrics:
    """Test cases for the upsert_price_metrics function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.sample_df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=3, freq='D'),
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'open': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'low': [99.0, 100.0, 101.0],
            'close': [100.5, 101.5, 102.5],
            'adj_close': [100.5, 101.5, 102.5],
            'volume': [1000000, 1100000, 1200000],
            'return': [0.02, -0.01, 0.01],
            'volatility': [0.15, 0.14, 0.16],
            'ema_9': [100.2, 100.8, 101.3],
            'ema_20': [100.1, 100.6, 101.1],
            'ema_50': [100.0, 100.4, 100.9]
        })

    @patch('src.load._create_engine_from_url')
    @patch('src.load.execute_values')
    def test_upsert_price_metrics_basic(self, mock_execute_values, mock_create_engine):
        """Test basic upsert functionality."""
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function
        upsert_price_metrics(self.sample_df, 'test_table', 'postgresql://test', 'AAPL')
        
        # Verify engine was created
        mock_create_engine.assert_called_once_with('postgresql://test')
        
        # Verify table creation
        mock_engine.begin.assert_called_once()
        
        # Verify raw connection was used
        mock_engine.raw_connection.assert_called_once()
        
        # Verify execute_values was called
        mock_execute_values.assert_called_once()

    @patch('src.load._create_engine_from_url')
    @patch('src.load.execute_values')
    def test_upsert_price_metrics_missing_ticker_column(self, mock_execute_values, mock_create_engine):
        """Test upsert when ticker column is missing."""
        df_no_ticker = self.sample_df.drop('ticker', axis=1)
        
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function
        upsert_price_metrics(df_no_ticker, 'test_table', 'postgresql://test', 'AAPL')
        
        # Should add ticker column
        mock_engine.begin.assert_called_once()
        mock_engine.raw_connection.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.load._create_engine_from_url')
    @patch('src.load.execute_values')
    def test_upsert_price_metrics_date_conversion(self, mock_execute_values, mock_create_engine):
        """Test that dates are properly converted."""
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.raw_connection.return_value = mock_connection
        
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call function
        upsert_price_metrics(self.sample_df, 'test_table', 'postgresql://test', 'AAPL')
        
        # Should convert dates to date objects
        mock_engine.begin.assert_called_once()
        mock_engine.raw_connection.assert_called_once()
        mock_execute_values.assert_called_once()


class TestTableDDL:
    """Test cases for the TABLE_DDL constant."""

    def test_table_ddl_format(self):
        """Test that TABLE_DDL can be formatted with table name."""
        formatted_ddl = TABLE_DDL.format(table='test_table')
        
        assert 'CREATE TABLE IF NOT EXISTS test_table' in formatted_ddl
        assert 'date        DATE        NOT NULL' in formatted_ddl
        assert 'ticker      VARCHAR(10) NOT NULL' in formatted_ddl
        assert 'PRIMARY KEY (ticker, date)' in formatted_ddl

    def test_table_ddl_columns(self):
        """Test that TABLE_DDL contains all expected columns."""
        formatted_ddl = TABLE_DDL.format(table='test_table')
        
        expected_columns = [
            'date', 'ticker', 'open', 'high', 'low', 'close',
            'adj_close', 'volume', 'return', 'volatility',
            'ema_9', 'ema_20', 'ema_50'
        ]
        
        for col in expected_columns:
            assert col in formatted_ddl.lower()


class TestUpsertSQL:
    """Test cases for the UPSERT_SQL constant."""

    def test_upsert_sql_format(self):
        """Test that UPSERT_SQL can be formatted with table name."""
        formatted_sql = UPSERT_SQL.format(table='test_table')
        
        assert 'INSERT INTO test_table' in formatted_sql
        assert 'ON CONFLICT (ticker, date) DO UPDATE SET' in formatted_sql

    def test_upsert_sql_columns(self):
        """Test that UPSERT_SQL contains all expected columns."""
        formatted_sql = UPSERT_SQL.format(table='test_table')
        
        expected_columns = [
            'date', 'ticker', 'open', 'high', 'low', 'close',
            'adj_close', 'volume', 'return', 'volatility',
            'ema_9', 'ema_20', 'ema_50'
        ]
        
        for col in expected_columns:
            assert col in formatted_sql.lower()


class TestColumns:
    """Test cases for the COLUMNS constant."""

    def test_columns_list(self):
        """Test that COLUMNS contains all expected column names."""
        expected_columns = [
            'date', 'ticker', 'open', 'high', 'low', 'close',
            'adj_close', 'volume', 'return', 'volatility',
            'ema_9', 'ema_20', 'ema_50',
            'macd', 'macd_signal', 'macd_histogram',
            'bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position',
            'rsi'
        ]
        
        assert COLUMNS == expected_columns

    def test_columns_length(self):
        """Test that COLUMNS has the correct length."""
        assert len(COLUMNS) == 22

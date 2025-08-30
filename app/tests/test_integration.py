"""
Integration tests for the stock data pipeline.

These tests verify that the extract, transform, and load modules work together
correctly as a complete pipeline. They use mocked external dependencies to
ensure tests can run without network access or database connections.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.extract import fetch_stock_data
from src.transform import calculate_financial_metrics
from src.load import load_to_postgres, upsert_price_metrics


class TestPipelineIntegration:
    """Test cases for the complete pipeline integration."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create sample stock data that mimics yfinance response
        self.sample_stock_data = pd.DataFrame({
            'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
            'High': [101.0, 102.0, 103.0, 104.0, 105.0],
            'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
            'Close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'Adj Close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'Volume': [1000000, 1100000, 1200000, 1300000, 1400000]
        }, index=pd.date_range('2023-01-01', periods=5, freq='D'))

        # Create multi-ticker data
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
        self.multi_ticker_data = multi_data

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_complete_pipeline_single_ticker(self, mock_execute_values, mock_create_engine, mock_download):
        """Test complete pipeline flow for a single ticker."""
        # Mock yfinance download
        mock_download.return_value = self.sample_stock_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline steps
        # 1. Extract
        extracted_data = fetch_stock_data('AAPL', start_date='2023-01-01', end_date='2023-01-05')
        
        # 2. Transform
        transformed_data = calculate_financial_metrics(extracted_data)
        
        # 3. Load
        load_to_postgres(transformed_data, 'price_metrics', {'host': 'localhost'})
        
        # Verify extract step
        assert isinstance(extracted_data, pd.DataFrame)
        assert len(extracted_data) == 5
        assert 'Open' in extracted_data.columns
        assert 'Close' in extracted_data.columns
        
        # Verify transform step
        assert isinstance(transformed_data, pd.DataFrame)
        assert len(transformed_data) == 4  # First row dropped due to NaN return
        assert 'return' in transformed_data.columns
        assert 'volatility' in transformed_data.columns
        assert 'ema_9' in transformed_data.columns
        assert 'ema_20' in transformed_data.columns
        assert 'ema_50' in transformed_data.columns
        
        # Verify load step was called
        mock_create_engine.assert_called_once()
        mock_engine.begin.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.extract.yf.download')
    @patch('src.load._create_engine_from_url')
    @patch('src.load.execute_values')
    def test_complete_pipeline_multiple_tickers(self, mock_execute_values, mock_create_engine, mock_download):
        """Test complete pipeline flow for multiple tickers."""
        # Mock yfinance download
        mock_download.return_value = self.multi_ticker_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline steps
        # 1. Extract
        extracted_data = fetch_stock_data(['AAPL', 'MSFT'], start_date='2023-01-01', end_date='2023-01-03')
        
        # 2. Transform
        transformed_data = calculate_financial_metrics(extracted_data)
        
        # 3. Load (using upsert for individual ticker)
        upsert_price_metrics(transformed_data, 'price_metrics', 'postgresql://test', 'AAPL')
        
        # Verify extract step
        assert isinstance(extracted_data, pd.DataFrame)
        assert len(extracted_data) == 3
        assert isinstance(extracted_data.columns, pd.MultiIndex)
        
        # Verify transform step
        assert isinstance(transformed_data, pd.DataFrame)
        assert len(transformed_data) == 4  # 2 tickers * 2 rows each after dropping NaN
        assert 'ticker' in transformed_data.columns
        assert 'AAPL' in transformed_data['ticker'].values
        assert 'MSFT' in transformed_data['ticker'].values
        
        # Verify load step was called
        mock_create_engine.assert_called_once()
        mock_engine.begin.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_pipeline_with_custom_parameters(self, mock_execute_values, mock_create_engine, mock_download):
        """Test pipeline with custom transformation parameters."""
        # Mock yfinance download
        mock_download.return_value = self.sample_stock_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline with custom parameters
        extracted_data = fetch_stock_data('AAPL', interval='1wk')
        transformed_data = calculate_financial_metrics(
            extracted_data, 
            ema_spans=[5, 10], 
            volatility_window=5
        )
        
        # For custom parameters, we need to test the transform step separately
        # since the load step expects the default column structure
        assert 'ema_5' in transformed_data.columns
        assert 'ema_10' in transformed_data.columns
        assert 'ema_9' not in transformed_data.columns
        assert 'ema_20' not in transformed_data.columns
        assert 'ema_50' not in transformed_data.columns
        
        # Verify yfinance was called with custom interval
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert call_args[1]['interval'] == '1wk'
        
        # Test load step with default parameters to ensure compatibility
        default_transformed_data = calculate_financial_metrics(extracted_data)
        load_to_postgres(default_transformed_data, 'price_metrics', {'host': 'localhost'})
        
        # Verify load step was called
        mock_create_engine.assert_called_once()
        mock_engine.begin.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_pipeline_error_handling(self, mock_execute_values, mock_create_engine, mock_download):
        """Test pipeline error handling and recovery."""
        # Mock yfinance download to fail
        mock_download.side_effect = Exception("Network error")
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Test that extract step properly handles errors
        with pytest.raises(Exception, match="Network error"):
            fetch_stock_data('INVALID_TICKER')
        
        # Test that transform step handles empty data gracefully
        empty_data = pd.DataFrame()
        metrics = calculate_financial_metrics(empty_data)
        assert metrics.empty
        
        # Test that load step handles empty data gracefully
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Create minimal valid data for load test
        minimal_data = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=1, freq='D'),
            'ticker': ['TEST'],
            'open': [100.0],
            'high': [101.0],
            'low': [99.0],
            'close': [100.5],
            'adj_close': [100.5],
            'volume': [1000000],
            'return': [0.0],
            'volatility': [0.0],
            'ema_9': [100.0],
            'ema_20': [100.0],
            'ema_50': [100.0]
        })
        
        # Should not raise exception
        load_to_postgres(minimal_data, 'price_metrics', {'host': 'localhost'})

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_pipeline_with_flatten_columns(self, mock_execute_values, mock_create_engine, mock_download):
        """Test that pipeline works correctly with flatten_columns=True."""
        # Mock yfinance download to return single ticker data
        mock_download.return_value = self.sample_stock_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline with flatten_columns=True (simulating dynamic DAG)
        extracted_data = fetch_stock_data('AAPL', flatten_columns=True)
        transformed_data = calculate_financial_metrics(extracted_data, validate_columns=True)
        
        # Verify that the ticker column is properly preserved
        assert 'ticker' in transformed_data.columns
        assert all(transformed_data['ticker'] == 'AAPL')
        
        # Verify data structure
        assert len(transformed_data) > 0
        expected_cols = {'date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'return', 'volatility'}
        assert expected_cols.issubset(set(transformed_data.columns))
        
        # Test load step
        load_to_postgres(transformed_data, 'price_metrics', {'host': 'localhost'})

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_pipeline_data_consistency(self, mock_execute_values, mock_create_engine, mock_download):
        """Test that data remains consistent through the pipeline."""
        # Mock yfinance download
        mock_download.return_value = self.sample_stock_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline
        extracted_data = fetch_stock_data('AAPL')
        transformed_data = calculate_financial_metrics(extracted_data)
        load_to_postgres(transformed_data, 'price_metrics', {'host': 'localhost'})
        
        # Verify data consistency
        # Original data has 5 rows
        assert len(self.sample_stock_data) == 5
        
        # Transformed data should have 4 rows (first row dropped due to NaN return)
        assert len(transformed_data) == 4
        
        # Verify that the first row in transformed data corresponds to second row in original
        original_second_row = self.sample_stock_data.iloc[1]
        transformed_first_row = transformed_data.iloc[0]
        
        assert abs(transformed_first_row['open'] - original_second_row['Open']) < 1e-10
        assert abs(transformed_first_row['close'] - original_second_row['Close']) < 1e-10
        assert abs(transformed_first_row['volume'] - original_second_row['Volume']) < 1e-10
        
        # Verify return calculation is correct
        # return = (close[1] - close[0]) / close[0] = (101.5 - 100.5) / 100.5 = 0.00995
        expected_return = (101.5 - 100.5) / 100.5
        assert abs(transformed_first_row['return'] - expected_return) < 1e-6

    @patch('src.extract.yf.download')
    @patch('src.load.create_db_engine')
    @patch('src.load.execute_values')
    def test_pipeline_column_mapping(self, mock_execute_values, mock_create_engine, mock_download):
        """Test that column names are properly mapped through the pipeline."""
        # Mock yfinance download
        mock_download.return_value = self.sample_stock_data
        
        # Mock database engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_connection = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        mock_engine.raw_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute pipeline
        extracted_data = fetch_stock_data('AAPL')
        transformed_data = calculate_financial_metrics(extracted_data)
        load_to_postgres(transformed_data, 'price_metrics', {'host': 'localhost'})
        
        # Verify column mapping
        # Original columns
        assert 'Open' in extracted_data.columns
        assert 'Close' in extracted_data.columns
        assert 'Volume' in extracted_data.columns
        
        # Transformed columns (lowercase)
        assert 'open' in transformed_data.columns
        assert 'close' in transformed_data.columns
        assert 'volume' in transformed_data.columns
        
        # Verify new calculated columns exist
        assert 'return' in transformed_data.columns
        assert 'volatility' in transformed_data.columns
        assert 'ema_9' in transformed_data.columns
        assert 'ema_20' in transformed_data.columns
        assert 'ema_50' in transformed_data.columns
        
        # Verify date and ticker columns
        assert 'date' in transformed_data.columns
        assert 'ticker' in transformed_data.columns

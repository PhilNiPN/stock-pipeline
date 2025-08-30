"""
Unit tests for the extract module.

Tests the fetch_stock_data function with mocked yfinance calls to avoid
external dependencies and network calls during testing.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.extract import fetch_stock_data


class TestFetchStockData:
    """Test cases for the fetch_stock_data function."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create sample data that mimics yfinance response
        self.sample_data = pd.DataFrame({
            'Open': [100.0, 101.0, 102.0],
            'High': [101.0, 102.0, 103.0],
            'Low': [99.0, 100.0, 101.0],
            'Close': [100.5, 101.5, 102.5],
            'Adj Close': [100.5, 101.5, 102.5],
            'Volume': [1000000, 1100000, 1200000]
        }, index=pd.date_range('2023-01-01', periods=3, freq='D'))

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_single_ticker(self, mock_download):
        """Test fetching data for a single ticker."""
        mock_download.return_value = self.sample_data
        
        result = fetch_stock_data('AAPL', start_date='2023-01-01', end_date='2023-01-03')
        
        # Verify yfinance was called correctly
        mock_download.assert_called_once_with(
            tickers=['AAPL'],
            start='2023-01-01',
            end='2023-01-03',
            interval='1d',
            group_by='column',
            auto_adjust=False,
            threads=True,
            progress=False
        )
        
        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert isinstance(result.index, pd.DatetimeIndex)

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_multiple_tickers(self, mock_download):
        """Test fetching data for multiple tickers."""
        # Create multi-index data for multiple tickers
        multi_data = pd.DataFrame({
            ('Open', 'AAPL'): [100.0, 101.0],
            ('High', 'AAPL'): [101.0, 102.0],
            ('Low', 'AAPL'): [99.0, 100.0],
            ('Close', 'AAPL'): [100.5, 101.5],
            ('Adj Close', 'AAPL'): [100.5, 101.5],
            ('Volume', 'AAPL'): [1000000, 1100000],
            ('Open', 'MSFT'): [200.0, 201.0],
            ('High', 'MSFT'): [201.0, 202.0],
            ('Low', 'MSFT'): [199.0, 200.0],
            ('Close', 'MSFT'): [200.5, 201.5],
            ('Adj Close', 'MSFT'): [200.5, 201.5],
            ('Volume', 'MSFT'): [2000000, 2100000]
        }, index=pd.date_range('2023-01-01', periods=2, freq='D'))
        multi_data.columns = pd.MultiIndex.from_tuples(multi_data.columns, names=['Field', 'Ticker'])
        
        mock_download.return_value = multi_data
        
        result = fetch_stock_data(['AAPL', 'MSFT'], start_date='2023-01-01', end_date='2023-01-02')
        
        # Verify yfinance was called correctly
        mock_download.assert_called_once_with(
            tickers=['AAPL', 'MSFT'],
            start='2023-01-01',
            end='2023-01-02',
            interval='1d',
            group_by='column',
            auto_adjust=False,
            threads=True,
            progress=False
        )
        
        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert isinstance(result.columns, pd.MultiIndex)

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_default_dates(self, mock_download):
        """Test fetching data with default dates."""
        mock_download.return_value = self.sample_data
        
        with patch('src.extract.datetime') as mock_datetime:
            mock_datetime.today.return_value = datetime(2023, 1, 15)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            result = fetch_stock_data('AAPL')
            
            # Should use default 5-year lookback and today's date
            expected_start = '2018-01-15'
            expected_end = '2023-01-15'
            
            mock_download.assert_called_once_with(
                tickers=['AAPL'],
                start=expected_start,
                end=expected_end,
                interval='1d',
                group_by='column',
                auto_adjust=False,
                threads=True,
                progress=False
            )

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_custom_interval(self, mock_download):
        """Test fetching data with custom interval."""
        mock_download.return_value = self.sample_data
        
        result = fetch_stock_data('AAPL', interval='1wk')
        
        # Verify the call was made with the correct interval
        call_args = mock_download.call_args
        assert call_args[1]['interval'] == '1wk'
        assert call_args[1]['tickers'] == ['AAPL']

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_empty_response(self, mock_download):
        """Test handling of empty data response."""
        mock_download.return_value = pd.DataFrame()
        
        result = fetch_stock_data('INVALID_TICKER')
        
        assert result.empty
        # Should not raise an exception

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_non_datetime_index(self, mock_download):
        """Test handling of non-datetime index."""
        # Create data with string index
        data_with_string_index = self.sample_data.copy()
        data_with_string_index.index = ['2023-01-01', '2023-01-02', '2023-01-03']
        
        mock_download.return_value = data_with_string_index
        
        result = fetch_stock_data('AAPL')
        
        # Should convert string index to datetime
        assert isinstance(result.index, pd.DatetimeIndex)

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_string_ticker_input(self, mock_download):
        """Test that string ticker is converted to list."""
        mock_download.return_value = self.sample_data
        
        result = fetch_stock_data('AAPL')
        
        # Verify ticker was converted to list
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert call_args[1]['tickers'] == ['AAPL']

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_list_ticker_input(self, mock_download):
        """Test that list ticker is handled correctly."""
        mock_download.return_value = self.sample_data
        
        result = fetch_stock_data(['AAPL', 'MSFT'])
        
        # Verify ticker list was passed correctly
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert call_args[1]['tickers'] == ['AAPL', 'MSFT']

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_tuple_ticker_input(self, mock_download):
        """Test that tuple ticker is converted to list."""
        mock_download.return_value = self.sample_data
        
        result = fetch_stock_data(('AAPL', 'MSFT'))
        
        # Verify ticker tuple was converted to list
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert call_args[1]['tickers'] == ['AAPL', 'MSFT']

    @patch('src.extract.yf.download')
    def test_fetch_stock_data_exception_handling(self, mock_download):
        """Test exception handling in fetch_stock_data."""
        mock_download.side_effect = Exception("Network error")
        
        with pytest.raises(Exception, match="Network error"):
            fetch_stock_data('AAPL')

    def test_fetch_stock_data_invalid_parameters(self):
        """Test validation of input parameters."""
        # Test with None tickers
        with pytest.raises(TypeError):
            fetch_stock_data(None)
        
        # Test with empty list
        with pytest.raises(ValueError):
            fetch_stock_data([])
        
        # Test with invalid date format - the function doesn't validate dates,
        # it just passes them to yfinance which handles the error
        # So we just test that it doesn't crash
        result = fetch_stock_data('AAPL', start_date='invalid-date')
        # Should return empty DataFrame when yfinance fails
        assert result.empty

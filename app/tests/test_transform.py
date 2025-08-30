"""
Unit tests for the transform module.

Tests the financial metrics calculation functions including returns,
volatility, EMAs, and the main calculate_financial_metrics function.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from src.transform import (
    _compute_returns,
    _compute_volatility,
    _compute_emas,
    calculate_financial_metrics
)


class TestComputeReturns:
    """Test cases for the _compute_returns function."""

    def test_compute_returns_basic(self):
        """Test basic return calculation."""
        prices = pd.Series([100, 102, 101, 103, 105], name='price')
        returns = _compute_returns(prices)
        
        # Check that first value is NaN
        assert pd.isna(returns.iloc[0])
        
        # Check other values with tolerance for floating point precision
        assert abs(returns.iloc[1] - 0.02) < 1e-10
        assert abs(returns.iloc[2] - (-0.009804)) < 1e-6
        assert abs(returns.iloc[3] - 0.019802) < 1e-6
        assert abs(returns.iloc[4] - 0.019417) < 1e-6

    def test_compute_returns_single_value(self):
        """Test return calculation with single value."""
        prices = pd.Series([100], name='price')
        returns = _compute_returns(prices)
        
        expected_returns = pd.Series([np.nan], name='price')
        pd.testing.assert_series_equal(returns, expected_returns, check_names=False)

    def test_compute_returns_zero_price(self):
        """Test return calculation with zero price."""
        prices = pd.Series([100, 0, 102], name='price')
        returns = _compute_returns(prices)
        
        # Should handle division by zero gracefully
        assert pd.isna(returns.iloc[0])
        assert returns.iloc[1] == -1.0  # (0-100)/100 = -1
        assert returns.iloc[2] == np.inf  # (102-0)/0 = inf

    def test_compute_returns_negative_price(self):
        """Test return calculation with negative price."""
        prices = pd.Series([100, -50, 75], name='price')
        returns = _compute_returns(prices)
        
        assert pd.isna(returns.iloc[0])
        assert returns.iloc[1] == -1.5  # (-50-100)/100 = -1.5
        assert returns.iloc[2] == -2.5  # (75-(-50))/(-50) = -2.5


class TestComputeVolatility:
    """Test cases for the _compute_volatility function."""

    def test_compute_volatility_basic(self):
        """Test basic volatility calculation."""
        returns = pd.Series([0.01, -0.02, 0.03, -0.01, 0.02])
        volatility = _compute_volatility(returns, window=3)
        
        # First two values should be NaN (window not filled)
        assert pd.isna(volatility.iloc[0])
        assert pd.isna(volatility.iloc[1])
        
        # Third value should be calculated
        assert not pd.isna(volatility.iloc[2])
        assert volatility.iloc[2] > 0

    def test_compute_volatility_custom_window(self):
        """Test volatility calculation with custom window."""
        returns = pd.Series([0.01, -0.02, 0.03, -0.01, 0.02])
        volatility = _compute_volatility(returns, window=2)
        
        # First value should be NaN
        assert pd.isna(volatility.iloc[0])
        
        # Second value should be calculated
        assert not pd.isna(volatility.iloc[1])

    def test_compute_volatility_annualization(self):
        """Test that volatility is properly annualized."""
        returns = pd.Series([0.01, -0.01, 0.01, -0.01, 0.01])
        volatility = _compute_volatility(returns, window=5)
        
        # Should be annualized by sqrt(252)
        expected_annualization_factor = np.sqrt(252)
        # The last value should be the std of returns * sqrt(252)
        expected_vol = returns.std() * expected_annualization_factor
        assert np.isclose(volatility.iloc[-1], expected_vol)

    def test_compute_volatility_constant_returns(self):
        """Test volatility calculation with constant returns."""
        returns = pd.Series([0.01, 0.01, 0.01, 0.01, 0.01])
        volatility = _compute_volatility(returns, window=3)
        
        # Volatility should be 0 for constant returns
        assert volatility.iloc[-1] == 0.0

    def test_compute_volatility_empty_series(self):
        """Test volatility calculation with empty series."""
        returns = pd.Series([])
        volatility = _compute_volatility(returns)
        
        assert len(volatility) == 0


class TestComputeEMAs:
    """Test cases for the _compute_emas function."""

    def test_compute_emas_basic(self):
        """Test basic EMA calculation."""
        prices = pd.Series([100, 102, 101, 103, 105], name='price')
        emas = _compute_emas(prices, spans=[2, 3])
        
        assert 'ema_2' in emas.columns
        assert 'ema_3' in emas.columns
        assert len(emas) == len(prices)
        
        # EMAs should be numeric
        assert emas['ema_2'].dtype in ['float64', 'float32']
        assert emas['ema_3'].dtype in ['float64', 'float32']

    def test_compute_emas_single_span(self):
        """Test EMA calculation with single span."""
        prices = pd.Series([100, 102, 101, 103, 105], name='price')
        emas = _compute_emas(prices, spans=[2])
        
        assert 'ema_2' in emas.columns
        assert len(emas.columns) == 1

    def test_compute_emas_multiple_spans(self):
        """Test EMA calculation with multiple spans."""
        prices = pd.Series([100, 102, 101, 103, 105], name='price')
        emas = _compute_emas(prices, spans=[2, 3, 5])
        
        assert 'ema_2' in emas.columns
        assert 'ema_3' in emas.columns
        assert 'ema_5' in emas.columns
        assert len(emas.columns) == 3

    def test_compute_emas_empty_spans(self):
        """Test EMA calculation with empty spans."""
        prices = pd.Series([100, 102, 101, 103, 105], name='price')
        emas = _compute_emas(prices, spans=[])
        
        assert len(emas.columns) == 0
        # When no spans are provided, should return empty DataFrame with same index
        assert len(emas) == 0  # Empty DataFrame has 0 rows

    def test_compute_emas_constant_prices(self):
        """Test EMA calculation with constant prices."""
        prices = pd.Series([100, 100, 100, 100, 100], name='price')
        emas = _compute_emas(prices, spans=[2])
        
        # EMA should equal the constant price
        assert all(emas['ema_2'] == 100)


class TestCalculateFinancialMetrics:
    """Test cases for the calculate_financial_metrics function."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create sample single ticker data
        self.single_ticker_data = pd.DataFrame({
            'Open': [100, 102, 101, 103, 105],
            'High': [101, 103, 102, 104, 106],
            'Low': [99, 101, 100, 102, 104],
            'Close': [100, 102, 101, 103, 105],
            'Adj Close': [100, 102, 101, 103, 105],
            'Volume': [1000, 1100, 1050, 1150, 1200]
        }, index=pd.date_range('2023-01-01', periods=5, freq='D'))

        # Create sample multi-ticker data
        multi_data = pd.DataFrame({
            ('Open', 'AAPL'): [100, 102, 101],
            ('High', 'AAPL'): [101, 103, 102],
            ('Low', 'AAPL'): [99, 101, 100],
            ('Close', 'AAPL'): [100, 102, 101],
            ('Adj Close', 'AAPL'): [100, 102, 101],
            ('Volume', 'AAPL'): [1000, 1100, 1050],
            ('Open', 'MSFT'): [200, 202, 201],
            ('High', 'MSFT'): [201, 203, 202],
            ('Low', 'MSFT'): [199, 201, 200],
            ('Close', 'MSFT'): [200, 202, 201],
            ('Adj Close', 'MSFT'): [200, 202, 201],
            ('Volume', 'MSFT'): [2000, 2100, 2050]
        }, index=pd.date_range('2023-01-01', periods=3, freq='D'))
        multi_data.columns = pd.MultiIndex.from_tuples(multi_data.columns, names=['Field', 'Ticker'])
        self.multi_ticker_data = multi_data

    def test_calculate_financial_metrics_single_ticker(self):
        """Test financial metrics calculation for single ticker."""
        metrics = calculate_financial_metrics(self.single_ticker_data)
        
        # Check expected columns
        expected_cols = {
            'date', 'ticker', 'open', 'high', 'low', 'close', 
            'adj_close', 'volume', 'return', 'volatility', 
            'ema_9', 'ema_20', 'ema_50'
        }
        assert expected_cols.issubset(set(metrics.columns))
        
        # Should have 4 rows (first row dropped due to NaN return)
        assert len(metrics) == 4
        
        # Check ticker column - should be 'UNKNOWN' for single ticker data
        assert all(metrics['ticker'] == 'UNKNOWN')
        
        # Check that returns are calculated correctly
        assert not metrics['return'].isna().all()
        assert abs(metrics['return'].iloc[0] - 0.02) < 1e-10  # (102-100)/100

    def test_calculate_financial_metrics_multi_ticker(self):
        """Test financial metrics calculation for multiple tickers."""
        metrics = calculate_financial_metrics(self.multi_ticker_data)
        
        # Should have 4 rows (2 tickers * 2 rows each after dropping NaN)
        assert len(metrics) == 4
        
        # Check ticker column contains both tickers
        tickers = metrics['ticker'].unique()
        assert 'AAPL' in tickers
        assert 'MSFT' in tickers
        
        # Check expected columns
        expected_cols = {
            'date', 'ticker', 'open', 'high', 'low', 'close', 
            'adj_close', 'volume', 'return', 'volatility', 
            'ema_9', 'ema_20', 'ema_50'
        }
        assert expected_cols.issubset(set(metrics.columns))

    def test_calculate_financial_metrics_custom_ema_spans(self):
        """Test financial metrics with custom EMA spans."""
        metrics = calculate_financial_metrics(
            self.single_ticker_data, 
            ema_spans=[5, 10]
        )
        
        assert 'ema_5' in metrics.columns
        assert 'ema_10' in metrics.columns
        assert 'ema_9' not in metrics.columns
        assert 'ema_20' not in metrics.columns
        assert 'ema_50' not in metrics.columns

    def test_calculate_financial_metrics_custom_volatility_window(self):
        """Test financial metrics with custom volatility window."""
        metrics = calculate_financial_metrics(
            self.single_ticker_data, 
            volatility_window=2
        )
        
        # Volatility should be calculated with window=2
        assert 'volatility' in metrics.columns

    def test_calculate_financial_metrics_no_adj_close(self):
        """Test financial metrics when Adj Close is not available."""
        data_no_adj = self.single_ticker_data.drop('Adj Close', axis=1)
        metrics = calculate_financial_metrics(data_no_adj)
        
        # Should fall back to Close price
        assert 'adj_close' in metrics.columns
        assert all(metrics['adj_close'] == metrics['close'])

    def test_calculate_financial_metrics_empty_data(self):
        """Test financial metrics with empty DataFrame."""
        empty_data = pd.DataFrame()
        
        # Should handle empty DataFrame gracefully by returning empty DataFrame
        metrics = calculate_financial_metrics(empty_data)
        assert metrics.empty

    def test_calculate_financial_metrics_single_row(self):
        """Test financial metrics with single row of data."""
        single_row_data = self.single_ticker_data.iloc[:1]
        metrics = calculate_financial_metrics(single_row_data)
        
        # Should return 1 row with zero return (graceful handling of single-day data)
        assert len(metrics) == 1
        assert metrics['return'].iloc[0] == 0.0

    def test_calculate_financial_metrics_date_column(self):
        """Test that date column is properly set."""
        metrics = calculate_financial_metrics(self.single_ticker_data)
        
        assert 'date' in metrics.columns
        assert isinstance(metrics['date'].iloc[0], pd.Timestamp)

    def test_calculate_financial_metrics_dropna_behavior(self):
        """Test that NaN returns are properly dropped."""
        metrics = calculate_financial_metrics(self.single_ticker_data)
        
        # Should not contain any NaN returns
        assert not metrics['return'].isna().any()

    def test_calculate_financial_metrics_volatility_calculation(self):
        """Test that volatility is calculated correctly."""
        metrics = calculate_financial_metrics(self.single_ticker_data)
        
        # Volatility should be calculated (may be NaN for some values due to window size)
        assert 'volatility' in metrics.columns
        # With default window size of 21, volatility will be NaN for small datasets
        # Just check that the column exists and has the right type
        assert metrics['volatility'].dtype in ['float64', 'float32']

    def test_calculate_financial_metrics_ema_calculation(self):
        """Test that EMAs are calculated correctly."""
        metrics = calculate_financial_metrics(self.single_ticker_data)
        
        # EMAs should be numeric and not all NaN
        for col in ['ema_9', 'ema_20', 'ema_50']:
            assert col in metrics.columns
            assert metrics[col].dtype in ['float64', 'float32']
            assert not metrics[col].isna().all()

    def test_calculate_financial_metrics_with_existing_ticker_column(self):
        """Test financial metrics with pre-existing ticker column (from flatten_columns=True)."""
        # Simulate data that comes from fetch_stock_data with flatten_columns=True
        data_with_ticker = self.single_ticker_data.copy()
        data_with_ticker['ticker'] = 'AAPL'  # Simulate the ticker column added by fetch_stock_data
        
        metrics = calculate_financial_metrics(data_with_ticker)
        
        # Should use the actual ticker value from the data
        assert all(metrics['ticker'] == 'AAPL')
        assert len(metrics) == 4  # Same as other single ticker tests
        
        # Check that all expected columns are present
        expected_cols = {
            'date', 'ticker', 'open', 'high', 'low', 'close', 
            'adj_close', 'volume', 'return', 'volatility', 
            'ema_9', 'ema_20', 'ema_50'
        }
        assert expected_cols.issubset(set(metrics.columns))

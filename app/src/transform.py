'''
Transformation logic for the stock data pipeline.

This module provides helpers to compute daily returns, rolling
annualised volatility, and a set of exponential moving averages (EMAs)
from historical equity prices. Returns are computed as the fractional
day-over-day change (via pandas .pct_change()), volatility is the
rolling standard deviation of daily returns scaled by sqrt(252), and
EMAs use pandas' exponentially weighted mean.

Given price data (multi-indexed as returned by yfinance.download 
with price field × ticker), the main function produces a wide, 
analysis-ready DataFrame: one row per timestamp and ticker, 
with columns for OHLCV, adjusted close (when available), return, 
volatility, and one column per requested EMA. The DataFrame
includes a 'date' column derived from the index and a 'ticker' column.
'''

from __future__ import annotations

import logging
from typing import Iterable, List

import pandas as pd

from .utils import setup_logging


logger = setup_logging(__name__)


def _compute_returns(series: pd.Series) -> pd.Series:
    '''Compute daily fractional returns for a price series.

    Uses pandas.Series.pct_change(), i.e. (price_t - price_{t-1}) / price_{t-1}.
    The first observation will be NaN due to the lack of a prior value.

    Parameters
    ----------
    series : pandas.Series
        Series of prices indexed by date.

    Returns
    -------
    pandas.Series
        Fractional daily returns (e.g., 0.01 == 1%).
    '''
    return series.pct_change()


def _compute_volatility(returns: pd.Series, window: int = 21) -> pd.Series:
    '''Compute rolling annualised volatility from daily returns.

    Volatility is calculated as the rolling standard deviation of daily
    returns over window periods, multiplied by sqrt(252) to annualise.
    The series will contain NaNs until the first full window is available.

    Parameters
    ----------
    returns : pandas.Series
        Series of daily fractional returns.
    window : int, optional
        Rolling window length. Defaults to 21 (≈ 1 trading month).

    Returns
    -------
    pandas.Series
        Rolling annualised volatility.
    '''
    # The rolling standard deviation multiplied by sqrt(252) yields annualised volatility
    return returns.rolling(window=window).std() * (252 ** 0.5)


def _compute_emas(series: pd.Series, spans: Iterable[int]) -> pd.DataFrame:
    '''Compute multiple exponential moving averages for a price series.

    Uses pandas.Series.ewm(..., span=span, adjust=False).mean() for each
    requested span. Column names follow the pattern 'ema_{span}'.

    Parameters
    ----------
    series : pandas.Series
        Price series.
    spans : iterable of int
        Window lengths for the EMAs.

    Returns
    -------
    pandas.DataFrame
        One column per EMA span (e.g., 'ema_9', 'ema_20').
    '''
    ema_frames = {}
    for span in spans:
        ema_frames[f'ema_{span}'] = series.ewm(span=span, adjust=False).mean()
    return pd.DataFrame(ema_frames)


def calculate_financial_metrics(
    data: pd.DataFrame,
    ema_spans: Iterable[int] = (9, 20, 50),
    volatility_window: int = 21,
    validate_columns: bool = False,
) -> pd.DataFrame:
    '''Compute returns, volatility, and EMAs per ticker and return a wide table.

    The input may hold one or many tickers. When multiple tickers are
    downloaded via yfinance, columns are usually a MultiIndex where the
    first level is the price field (e.g., 'Adj Close', 'Close', 'Open')
    and the second level is the ticker symbol. For each ticker, this
    function:
    1. selects the price series (prefers 'Adj Close' if present, otherwise 'Close') for computations
    2. computes daily fractional returns via pct_change()
    3. computes rolling annualised volatility over volatility_window
    4. computes EMAs for each 'ema_spans' value
    5. assembles a wide DataFrame with OHLCV, adj_close (or close)
       return, volatility, EMA columns, plus 'ticker' and 'date'.
    
    Parameters
    ----------
    data : pandas.DataFrame
        Raw price data (e.g., from yfinance.download). May be single- or
        multi-indexed across columns.
    ema_spans : iterable of int, optional
        EMA spans to compute. Defaults to (9, 20, 50).
    volatility_window : int, optional
        Window length for rolling volatility. Defaults to 21.
    validate_columns : bool, optional
        If True, validates required OHLCV columns before processing. Useful for
        single-ticker processing or when you need strict validation. Defaults to False.

    Returns
    -------
    pandas.DataFrame
        Wide DataFrame with columns:
        ['date', 'ticker', 'open', 'high', 'low', 'close',
         'adj_close', 'volume', 'return', 'volatility', ema_span].
    '''
    if data.empty:
        logger.warning('No data to process')
        return pd.DataFrame()
    
    frames: List[pd.DataFrame] = []

    # Determine if the DataFrame has a multi‑index in columns
    if isinstance(data.columns, pd.MultiIndex):
        tickers = data.columns.get_level_values(1).unique()
        has_multi_index = True
    else:
        # Check if there's already a ticker column (from flatten_columns=True)
        if 'ticker' in data.columns:
            tickers = data['ticker'].unique()
            has_multi_index = False
        else:
            # Single ticker; use generic name
            tickers = ['']
            has_multi_index = False

    for ticker in tickers:
        if has_multi_index and ticker:
            # Slice data for the ticker across the first level of the column multi‑index
            sub = data.xs(ticker, axis=1, level=1)
        else:
            # For single ticker or flattened data, use the whole DataFrame
            sub = data

        # Validate columns if requested
        if validate_columns:
            logger.info(f'Processing ticker {ticker} with columns: {list(sub.columns)}')
            
            # Check for required OHLCV columns
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_required = [col for col in required_columns if col not in sub.columns]
            
            if missing_required:
                logger.error(f'Ticker {ticker} is missing required OHLCV columns: {missing_required}')
                logger.error(f'Available columns: {list(sub.columns)}')
                logger.error(f'Cannot process ticker without basic price data')
                continue

        # Ensure we have expected columns
        # Use Adjusted Close if available, otherwise fall back to Close
        price_col = 'Adj Close' if 'Adj Close' in sub.columns else 'Close'
        
        if validate_columns:
            logger.info(f'Using {price_col} for calculations for ticker {ticker}')

        try:
            returns = _compute_returns(sub[price_col])
            vol = _compute_volatility(returns, window=volatility_window)
            emas = _compute_emas(sub[price_col], spans=ema_spans)

            # Build result DataFrame
            result = pd.DataFrame(
                {
                    'open': sub['Open'],
                    'high': sub['High'],
                    'low': sub['Low'],
                    'close': sub['Close'],
                    'adj_close': sub[price_col],
                    'volume': sub['Volume'],
                    'return': returns,
                    'volatility': vol,
                }
            )

            result = pd.concat([result, emas], axis=1)
            # Use the actual ticker value, or get it from the data if available
            if ticker:
                result['ticker'] = ticker
            elif 'ticker' in data.columns:
                result['ticker'] = data['ticker'].iloc[0] if len(data) > 0 else 'UNKNOWN'
            else:
                result['ticker'] = tickers[0] if tickers and tickers[0] else 'UNKNOWN'
            
            # Debug: Log index information for multi-ticker processing
            logger.info(f'Multi-ticker index type for {ticker}: {type(result.index)}')
            logger.info(f'Multi-ticker index sample for {ticker}: {result.index[:3]}')
            
            # Properly format the date column from the datetime index
            if isinstance(result.index, pd.DatetimeIndex):
                result['date'] = result.index
            else:
                # Fallback: try to convert index to datetime first
                logger.warning(f'Multi-ticker index is not DatetimeIndex for {ticker}, attempting conversion')
                result['date'] = pd.to_datetime(result.index)
                
            frames.append(result.reset_index(drop=True))
            
        except Exception as e:
            logger.error(f'Error calculating metrics for ticker {ticker}: {e}')
            if validate_columns:
                # In validation mode, skip this ticker and continue with others
                continue
            else:
                # In non-validation mode, re-raise the exception
                raise

    if not frames:
        logger.warning('No valid data frames to concatenate')
        return pd.DataFrame()
        
    metrics = pd.concat(frames, ignore_index=True)
    
    # Handle NaN returns more gracefully for new tickers or single-day data
    initial_rows = len(metrics)
    metrics_with_returns = metrics.dropna(subset=['return']).reset_index(drop=True)
    
    if metrics_with_returns.empty and initial_rows > 0:
        # If we have data but no valid returns, keep the data with zero returns
        logger.warning(f'No valid returns calculated, keeping raw data with zero returns for {initial_rows} rows')
        metrics['return'] = metrics['return'].fillna(0.0)
        metrics = metrics.reset_index(drop=True)
    else:
        metrics = metrics_with_returns

    logger.info('Calculated metrics for %d rows', len(metrics))
    return metrics

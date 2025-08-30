'''
Extraction logic for the stock data pipeline.

This module exposes a helper to fetch historical price data for one or
more tickers using the yfinance API. If start/end dates are not given,
it defaults to the last 5 years up to today, with a daily interval
(by default).

The returned DataFrame contains the standard OHLCV fields plus
'Adj Close'. For multiple tickers, columns are a MultiIndex where the
first level is the price field ('Open', 'High', 'Low', 'Close',
'Adj Close', 'Volume') and the second level is the ticker symbol
(group_by='column'). The index is ensured to be a pandas DatetimeIndex.
'''

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Optional, Union

import pandas as pd
import yfinance as yf

from .utils import setup_logging
from dotenv import load_dotenv

load_dotenv()


logger = setup_logging(__name__)


def fetch_stock_data(
    tickers: Union[str, Iterable[str]],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    interval: str = '1d',
    flatten_columns: bool = False,
) -> pd.DataFrame:
    '''Fetch historical OHLCV (and Adj Close) for one or more tickers via yfinance.

    Behavior
    --------
    - Accepts a single ticker (str) or an iterable of tickers.
    - If start_date is not provided, uses "today minus 5 years".
    - If end_date is not provided, uses today's date.
    - Uses group_by='column' so that, for multiple tickers, columns form a
      MultiIndex with price field as level 0 and ticker as level 1.
    - Sets auto_adjust=False (raw Yahoo values; 'Adj Close' is included as its own column).
    - Ensures the index is a pandas.DatetimeIndex.
    - Logs a warning if no data is retrieved.
    - Validates required OHLCV columns when flatten_columns=True.

    Parameters
    ----------
    tickers : str or iterable of str
        A single symbol (e.g., 'AAPL') or an iterable of symbols.
    start_date : str, optional
        Inclusive start date in 'YYYY-MM-DD' format. Defaults to 5 years ago if omitted.
    end_date : str, optional
        Exclusive end date in 'YYYY-MM-DD' format. Defaults to today if omitted.
    interval : str, optional
        Sampling interval (e.g., '1d', '1wk', '1mo'). See yfinance docs for the full set.
    flatten_columns : bool, optional
        If True, flattens MultiIndex columns and adds ticker column. Useful for single-ticker
        processing or when you need flat column structure. Defaults to False.

    Returns
    -------
    pandas.DataFrame
        Price DataFrame indexed by date. For multiple tickers, columns are a
        MultiIndex: level 0 = price field, level 1 = ticker (unless flatten_columns=True).

    '''

    if isinstance(tickers, str):
        tickers_list = [tickers]
        single_ticker = True
    else:
        tickers_list = list(tickers)
        single_ticker = len(tickers_list) == 1

    if not start_date:
        # Default to five years of data if not specified
        start_date = (datetime.today().replace(year=datetime.today().year - 5)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.today().strftime('%Y-%m-%d')

    logger.info(
        'Fetching data for %s from %s to %s at %s intervals',
        ','.join(tickers_list),
        start_date,
        end_date,
        interval,
    )

    # Download the data. Passing a list returns a DataFrame with multi-indexed columns
    # (level 0 = price field, level 1 = ticker) when group_by='column'.
    data = yf.download(
        tickers=tickers_list,
        start=start_date,
        end=end_date,
        interval=interval,
        group_by='column',  # unify format across single and multi ticker
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    if data.empty:
        logger.warning('No data was retrieved for the given parameters.')
        return pd.DataFrame()
    
    # Log column information for debugging
    logger.info(f'Retrieved data shape: {data.shape}')
    logger.info(f'Retrieved columns: {list(data.columns)}')
    if hasattr(data.columns, 'levels'):
        logger.info(f'MultiIndex columns - levels: {data.columns.levels}')

    # Ensure the index is of type datetime for downstream operations
    if not isinstance(data.index, pd.DatetimeIndex):
        data.index = pd.to_datetime(data.index)

    # Handle column flattening if requested
    if flatten_columns:
        if isinstance(data.columns, pd.MultiIndex):
            logger.info(f'Flattening MultiIndex columns')
            # Flatten MultiIndex columns - take the first level (price field names)
            data.columns = data.columns.get_level_values(0)
            logger.info(f'Flattened columns: {list(data.columns)}')
        
        # Check if we have the expected price columns
        expected_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in expected_columns if col not in data.columns]
        
        if missing_columns:
            logger.error(f'Missing required columns: {missing_columns}')
            logger.error(f'Available columns: {list(data.columns)}')
            logger.error(f'This usually indicates an invalid ticker symbol or delisted stock')
            return pd.DataFrame()
        
        # Add the ticker column for consistency
        if single_ticker and 'ticker' not in data.columns:
            data['ticker'] = tickers_list[0]

    return data


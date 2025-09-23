'''
Loading logic for the stock data pipeline.

This module creates (if needed) and upserts price/indicator rows into a
PostgreSQL table using a primary key on (ticker, date). It:

- Builds a SQLAlchemy engine (via utils.create_db_engine or a db_url),
- Ensures the target table exists with the expected schema,
- Uses psycopg2's execute_values for fast, batched UPSERT (ON CONFLICT),
- De-duplicates rows within the current batch to reduce conflicts.

The schema includes OHLCV, adj_close, return, volatility, and EMA columns. 
'''

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine as _create_engine_from_url
from psycopg2.extras import execute_values

from .utils import create_db_engine, setup_logging

logger = setup_logging(__name__)

TABLE_DDL = '''
CREATE TABLE IF NOT EXISTS {table} (
    date        DATE        NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    open        NUMERIC(18,6),
    high        NUMERIC(18,6),
    low         NUMERIC(18,6),
    close       NUMERIC(18,6),
    adj_close   NUMERIC(18,6),
    volume      BIGINT,
    return      NUMERIC(18,10),
    volatility  NUMERIC(18,10),
    ema_9       NUMERIC(18,6),
    ema_20      NUMERIC(18,6),
    ema_50      NUMERIC(18,6),
    -- MACD indicators
    macd        NUMERIC(18,10),
    macd_signal NUMERIC(18,10),
    macd_histogram NUMERIC(18,10),
    -- Bollinger Bands
    bb_middle   NUMERIC(18,6),
    bb_upper    NUMERIC(18,6),
    bb_lower    NUMERIC(18,6),
    bb_width    NUMERIC(18,10),
    bb_position NUMERIC(18,10),
    -- RSI
    rsi         NUMERIC(18,6),
    PRIMARY KEY (ticker, date)
);
'''

UPSERT_SQL = '''
INSERT INTO {table} (
    date, ticker, open, high, low, close, adj_close, volume,
    return, volatility, ema_9, ema_20, ema_50,
    macd, macd_signal, macd_histogram,
    bb_middle, bb_upper, bb_lower, bb_width, bb_position,
    rsi
) VALUES %s
ON CONFLICT (ticker, date) DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    adj_close  = EXCLUDED.adj_close,
    volume     = EXCLUDED.volume,
    return     = EXCLUDED.return,
    volatility = EXCLUDED.volatility,
    ema_9      = EXCLUDED.ema_9,
    ema_20     = EXCLUDED.ema_20,
    ema_50     = EXCLUDED.ema_50,
    macd       = EXCLUDED.macd,
    macd_signal = EXCLUDED.macd_signal,
    macd_histogram = EXCLUDED.macd_histogram,
    bb_middle  = EXCLUDED.bb_middle,
    bb_upper   = EXCLUDED.bb_upper,
    bb_lower   = EXCLUDED.bb_lower,
    bb_width   = EXCLUDED.bb_width,
    bb_position = EXCLUDED.bb_position,
    rsi        = EXCLUDED.rsi;
'''

COLUMNS = [
    'date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume',
    'return', 'volatility', 'ema_9', 'ema_20', 'ema_50',
    'macd', 'macd_signal', 'macd_histogram',
    'bb_middle', 'bb_upper', 'bb_lower', 'bb_width', 'bb_position',
    'rsi'
]

def _to_records(df: pd.DataFrame) -> List[Tuple]:
    '''Convert a metrics DataFrame to an ordered list of tuples for execute_values.

    Ensures:
      - 'date' is converted to Python date objects,
      - columns are in the expected COLUMNS order,
      - missing columns are filled with None values.

    Parameters
    ----------
    df : pandas.DataFrame
        Data with at least 'date' and 'ticker' columns. Missing columns from 
        COLUMNS will be filled with None values.

    Returns
    -------
    list[tuple]
        Row tuples suitable for psycopg2.extras.execute_values.
    '''
    out = df.copy()
    out['date'] = pd.to_datetime(out['date']).dt.date
    
    # Add any missing columns with None values
    for col in COLUMNS:
        if col not in out.columns:
            out[col] = None
    
    out = out[COLUMNS]
    return list(map(tuple, out.itertuples(index=False, name=None)))

def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    db_config: Dict[str, str],
    chunksize: int = 5_000,
) -> None:
    '''
    This function is used to load the data into the PostgreSQL database.
    Idempotent load: create table with PK and UPSERT rows in batches.
    
    Behavior:
      - Builds an engine from db_config using utils.create_db_engine,
      - Creates the table if it does not exist (TABLE_DDL),
      - Drops in-batch duplicates by (ticker, date),
      - Batches rows and performs an ON CONFLICT (ticker, date) DO UPDATE UPSERT
        using psycopg2.execute_values for speed.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to persist. Must contain 'date', 'ticker', OHLCV,
        and metric columns matching the target schema.
    table_name : str
        Name of the target table.
    db_config : dict
        Connection parameters used by utils.create_db_engine
        (e.g., host, port, user, password, database, driver).
    chunksize : int, optional
        Number of rows per batch for execute_values. Defaults to 5,000.

    Returns
    -------
    None
    '''
    # Validate table name to prevent SQL injection (only allow alphanumeric and underscore)
    if not table_name.replace('_', '').isalnum():
        raise ValueError(f"Invalid table name: {table_name}")
        
    engine = create_db_engine(db_config)
    logger.info("Preparing to load %d rows into '%s'", len(df), table_name)

    # Drop duplicates within this batch to reduce needless conflicts
    df = df.drop_duplicates(subset=['ticker', 'date']).copy()
    if df.empty:
        logger.info('No rows to load after in-batch de-duplication.')
        return

    create_sql = TABLE_DDL.format(table=table_name)
    upsert_sql = UPSERT_SQL.format(table=table_name)

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # Use psycopg2 connection for fast execute_values
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            records = _to_records(df)
            for i in range(0, len(records), chunksize):
                batch = records[i : i + chunksize]
                execute_values(cur, upsert_sql, batch)
        raw.commit()
    finally:
        raw.close()

    logger.info("Load complete (upserted into '%s')", table_name)


def upsert_price_metrics(df: pd.DataFrame, table: str, db_url: str, ticker: str) -> None:
    '''Compatibility wrapper used by the Airflow DAG.

    Ensures the table exists, coerces 'date' to date objects, adds a
    'ticker' column if missing, then performs an UPSERT using psycopg2.execute_values.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Metrics for a single ticker. If the 'ticker' column is missing it will be added.
    table : str
        Target table name.
    db_url : str
        Full SQLAlchemy database URL, e.g. 'postgresql+psycopg2://user:pass@host:5432/db'.
    ticker : str
        Ticker symbol for the provided metrics.
    '''
    # Validate table name to prevent SQL injection (only allow alphanumeric and underscore)
    if not table.replace('_', '').isalnum():
        raise ValueError(f"Invalid table name: {table}")
        
    out = df.copy()
    if 'ticker' not in out.columns:
        out['ticker'] = ticker
    if 'date' in out.columns:
        out['date'] = pd.to_datetime(out['date']).dt.date

    create_sql = TABLE_DDL.format(table=table)
    upsert_sql = UPSERT_SQL.format(table=table)

    engine = _create_engine_from_url(db_url)
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            records = _to_records(out)
            execute_values(cur, upsert_sql, records)
        raw.commit()
    finally:
        raw.close()

    logger.info("Upserted %d rows for %s into '%s'", len(out), ticker, table)


def load_single_ticker_to_postgres(
    df: pd.DataFrame,
    ticker: str,
    table_name: str,
    db_config: Dict[str, str],
    chunksize: int = 5_000,
) -> None:
    '''
    Load data for a single ticker into PostgreSQL (optimized for dynamic task mapping).
    
    This function is designed for use with Airflow's dynamic task mapping,
    where each ticker is processed in a separate task instance.
    
    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to persist for a single ticker.
    ticker : str
        Ticker symbol for identification and validation.
    table_name : str
        Name of the target table.
    db_config : dict
        Connection parameters used by utils.create_db_engine.
    chunksize : int, optional
        Number of rows per batch for execute_values. Defaults to 5,000.
        
    Returns
    -------
    None
    '''
    if df.empty:
        logger.info(f'No data to load for ticker {ticker}')
        return
    
    # Ensure ticker column exists and matches the expected ticker
    if 'ticker' not in df.columns:
        df = df.copy()
        df['ticker'] = ticker
    elif df['ticker'].iloc[0] != ticker:
        logger.warning(f'Ticker mismatch: expected {ticker}, found {df["ticker"].iloc[0]}')
        df = df.copy()
        df['ticker'] = ticker
    
    # Use the existing load function
    load_to_postgres(df, table_name, db_config, chunksize)
    logger.info(f'Successfully loaded {len(df)} rows for ticker {ticker}')

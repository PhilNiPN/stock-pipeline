'''
Utility functions for the stock data pipeline.

This module centralises helpers for reading environment variables,
creating database engines, and initialising logging. The functions
defined here are used across the extract, transform and load
components of the pipeline. Keeping these helpers in a dedicated
module improves testability and avoids duplication.
'''

from __future__ import annotations

import logging
import pandas as pd
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy import text


def get_next_start_dates(db_config: Dict[str, str], table: str, tickers: List[str]) -> dict[str, str | None]:
    '''
    Return {ticker: next_start_date_str}, where next_start_date_str is (max(date)+1 day) in 'YYYY-MM-DD'.
    If a ticker has no rows yet, value is None.
    '''
    engine = create_db_engine(db_config)
    latest: dict[str, pd.Timestamp] = {}
    try:
        with engine.connect() as conn:
            # Validate table name to prevent SQL injection (only allow alphanumeric and underscore)
            if not table.replace('_', '').isalnum():
                raise ValueError(f"Invalid table name: {table}")
            sql = text(f'SELECT ticker, MAX(date) AS max_date FROM {table} GROUP BY ticker')
            for ticker, max_date in conn.execute(sql):
                latest[ticker] = pd.to_datetime(max_date)
    except Exception:
        # Table likely doesn't exist yet—treat as cold start for all tickers
        pass

    out: dict[str, str | None] = {}
    for t in tickers:
        d = latest.get(t)
        out[t] = (d + pd.Timedelta(days=1)).strftime('%Y-%m-%d') if d is not None else None
    return out


def get_db_config_from_airflow_connection(conn_id: str = 'postgres_default') -> Dict[str, str]:
    '''
    Get database configuration from an Airflow connection.
    
    This function retrieves database connection parameters from Airflow's 
    connection management system instead of environment variables. This is
    the preferred approach for production Airflow deployments as it provides
    better security and centralized connection management.
    
    Parameters
    ----------
    conn_id : str, optional
        The Airflow connection ID. Defaults to 'postgres_default'.
        
    Returns
    -------
    dict
        A dictionary with keys 'host', 'port', 'database', 'user' and 'password'.
        
    Raises
    ------
    ImportError
        If Airflow is not available (e.g., running outside Airflow context).
    Exception
        If the connection cannot be found or retrieved.
    '''
    try:
        from airflow.hooks.base import BaseHook
        
        # Get the connection from Airflow
        connection = BaseHook.get_connection(conn_id)
        
        return {
            'host': connection.host or 'db',
            'port': str(connection.port or 5432),
            'database': connection.schema or 'stockdb',
            'user': connection.login or 'postgres',
            'password': connection.password or 'postgres'
        }
    except ImportError:
        raise ImportError(
            "Airflow is not available. This function can only be used within an Airflow environment."
        )
    except Exception as e:
        raise Exception(f"Failed to retrieve connection '{conn_id}': {e}")


def get_db_config_with_fallback(conn_id: str = 'postgres_default', prefix: str = 'DB') -> Dict[str, str]:
    '''
    Get database configuration with Airflow connection fallback to environment variables.
    
    This function first tries to get database configuration from Airflow connections.
    If that fails (e.g., outside Airflow context), it falls back to environment variables.
    This provides compatibility for both Airflow and standalone execution.
    
    Parameters
    ----------
    conn_id : str, optional
        The Airflow connection ID. Defaults to 'postgres_default'.
    prefix : str, optional
        The environment variable prefix for fallback. Defaults to 'DB'.
        
    Returns
    -------
    dict
        A dictionary with keys 'host', 'port', 'database', 'user' and 'password'.
    '''
    try:
        # Try to get configuration from Airflow connection first
        return get_db_config_from_airflow_connection(conn_id)
    except (ImportError, Exception) as e:
        # Fall back to environment variables
        logging.getLogger(__name__).info(
            f"Failed to get config from Airflow connection '{conn_id}': {e}. "
            "Falling back to environment variables."
        )
        return get_db_config(prefix)


def get_db_config(prefix: str = 'DB') -> Dict[str, str]:
    '''Construct a dictionary of database connection parameters.

    Connection details are primarily sourced from environment variables.
    If a full SQLAlchemy URL is provided via 'DB_URL' this function
    will parse it and return its components. Otherwise the individual
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER' and
    'DB_PASSWORD' variables are used. When running inside Docker
    compose, the database service is typically named 'db' so we
    default the host accordingly instead of 'localhost'.

    Parameters
    ----------
    prefix : str, optional
        The environment variable prefix. Defaults to DB.

    Returns
    -------
    dict
        A dictionary with keys 'host', 'port', 'database',
        'user' and 'password'. If the URL cannot be parsed
        successfully the function falls back to sensible defaults.
    '''
    # Prefer a full DB_URL if present. This allows a single env var to
    # specify all connection details (e.g. from docker‑compose). A URL
    # takes precedence over individual parts because it may include
    # driver hints like 'postgresql+psycopg2'. We strip the
    # driver+scheme portion and parse the remainder using urllib.
    db_url = os.getenv('DB_URL')
    if db_url:
        try:
            from urllib.parse import urlparse

            parsed = urlparse(db_url)
            # Ensure that user/password may not be present
            username = parsed.username or os.getenv(f'{prefix}_USER', 'postgres')
            password = parsed.password or os.getenv(f'{prefix}_PASSWORD', 'postgres')
            hostname = parsed.hostname or os.getenv(f'{prefix}_HOST', 'db')
            port = parsed.port or int(os.getenv(f'{prefix}_PORT', 5432))
            # Path begins with a slash; strip it
            database = (parsed.path or '/stockdb').lstrip('/')
            return {
                'host': hostname,
                'port': int(port),
                'database': database,
                'user': username,
                'password': password,
            }
        except Exception:
            # Fall back to individual pieces if parsing fails
            pass

    # Fallback: read individual connection parameters. Use 'db' as a
    # default host when DB_HOST is unspecified, since in docker-compose,
    # the Postgres host is called 'db'. If not running in Docker this can be overridden via environment.
    return {
        'host': os.getenv(f'{prefix}_HOST', os.getenv('DB_HOST', 'db')),
        'port': int(os.getenv(f'{prefix}_PORT', os.getenv('DB_PORT', 5432))),
        'database': os.getenv(f'{prefix}_NAME', os.getenv('DB_NAME', 'stockdb')),
        'user': os.getenv(f'{prefix}_USER', os.getenv('DB_USER', 'postgres')),
        'password': os.getenv(f'{prefix}_PASSWORD', os.getenv('DB_PASSWORD', 'postgres')),
    }


def create_db_engine(config: Dict[str, str]):
    '''Create a SQLAlchemy engine for a PostgreSQL database.

    The returned engine can be passed directly to pandas
    :meth:'DataFrame.to_sql' which writes records to a table. The
    pandas documentation notes that 'to_sql' supports SQLAlchemy
    engines and automatically creates or appends to tables depending
    on the 'if_exists' argument.

    Parameters
    ----------
    config: dict
        Dictionary containing connection parameters (host, port,
        database, user, password).

    Returns
    -------
    sqlalchemy.engine.base.Engine
        A SQLAlchemy engine configured for PostgreSQL.
    '''
    from urllib.parse import quote_plus
    
    user = quote_plus(config['user'])
    password = quote_plus(config['password'])
    host = config['host']
    port = config['port']
    database = config['database']
    url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    return create_engine(url)


def setup_logging(name: str = 'stock_pipeline') -> logging.Logger:
    '''Set up a logger with a default configuration.
    
    Parameters:
    ----------
    name: str, optional
        The logger name. Defaults to 'stock_pipeline'.

    Returns:
    -------
    logging.Logger
        Configured logger.
    '''
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def validate_data_quality(
    db_config: Dict[str, str],
    table_name: str,
    tickers_processed: List[str],
    execution_date: datetime,
    lookback_days: int = 30
) -> Dict[str, Any]:
    '''
    Perform comprehensive data quality validation after pipeline execution.
    
    Validations performed:
    1. Row count > 0: Ensure each ticker has data loaded
    2. Date continuity: Check for gaps in date sequences per ticker
    3. Ticker whitelist: Verify all processed tickers are in allowed list
    
    Parameters
    ----------
    db_config : dict
        Database connection configuration
    table_name : str
        Target table name to validate
    tickers_processed : list
        List of tickers that were processed
    execution_date : datetime
        Pipeline execution date
    lookback_days : int, optional
        Number of days to look back for continuity check. Defaults to 30.
        
    Returns
    -------
    dict
        Validation results with status and details
    '''
    logger = logging.getLogger(__name__)
    validation_results = {
        'status': 'success',
        'validations': {},
        'errors': [],
        'warnings': []
    }
    
    try:
        engine = create_db_engine(db_config)
        
        # 1. Row count validation (> 0)
        logger.info('Validating row counts for processed tickers')
        row_count_validation = {'status': 'success', 'details': {}}
        
        for ticker in tickers_processed:
            query = text(f'''
                SELECT COUNT(*) as row_count 
                FROM {table_name} 
                WHERE ticker = :ticker
            ''')
            
            with engine.connect() as conn:
                result = conn.execute(query, {'ticker': ticker})
                row_count = result.scalar()
                
                row_count_validation['details'][ticker] = row_count
                
                if row_count == 0:
                    row_count_validation['status'] = 'failed'
                    validation_results['errors'].append(f'Ticker {ticker}: No rows found in database')
                    logger.error(f'Ticker {ticker}: No rows found in database')
                else:
                    logger.info(f'Ticker {ticker}: {row_count} rows found')
        
        validation_results['validations']['row_count'] = row_count_validation
        
        # 2. Date continuity validation
        logger.info('Validating date continuity for processed tickers')
        continuity_validation = {'status': 'success', 'details': {}}
        
        for ticker in tickers_processed:
            # Get date range to check
            end_date = execution_date.date()
            start_date = end_date - timedelta(days=lookback_days)
            
            query = text(f'''
                SELECT date, 
                       LAG(date) OVER (ORDER BY date) as prev_date
                FROM {table_name} 
                WHERE ticker = :ticker 
                  AND date >= :start_date 
                  AND date <= :end_date
                ORDER BY date
            ''')
            
            with engine.connect() as conn:
                result = conn.execute(query, {
                    'ticker': ticker,
                    'start_date': start_date,
                    'end_date': end_date
                })
                rows = result.fetchall()
                
                gaps = []
                for i, row in enumerate(rows):
                    if i > 0:  # Skip first row (no previous date)
                        current_date = row[0]
                        prev_date = row[1]
                        
                        if prev_date and (current_date - prev_date).days > 1:
                            gaps.append({
                                'gap_start': prev_date,
                                'gap_end': current_date,
                                'days_missing': (current_date - prev_date).days - 1
                            })
                
                continuity_validation['details'][ticker] = {
                    'total_dates': len(rows),
                    'gaps': gaps,
                    'date_range': f'{start_date} to {end_date}'
                }
                
                if gaps:
                    continuity_validation['status'] = 'warning'
                    validation_results['warnings'].append(
                        f'Ticker {ticker}: Found {len(gaps)} date gaps in last {lookback_days} days'
                    )
                    logger.warning(f'Ticker {ticker}: Found {len(gaps)} date gaps')
                else:
                    logger.info(f'Ticker {ticker}: No date gaps found in last {lookback_days} days')
        
        validation_results['validations']['date_continuity'] = continuity_validation
        
        # 3. Ticker whitelist validation
        logger.info('Validating tickers against whitelist')
        whitelist_validation = {'status': 'success', 'details': {}}
        
        # Get allowed tickers from Airflow Variables (with fallback)
        try:
            from airflow.models import Variable
            allowed_tickers_str = Variable.get('TICKERS', 'NVDA,AAPL,MSFT,GOOGL', deserialize_json=False)
            if isinstance(allowed_tickers_str, str):
                allowed_tickers = [t.strip() for t in allowed_tickers_str.split(',') if t.strip()]
            else:
                allowed_tickers = allowed_tickers_str
        except ImportError:
            # Fallback to environment variable if not in Airflow context
            import os
            allowed_tickers_str = os.getenv('TICKERS', 'NVDA,AAPL,MSFT,GOOGL')
            allowed_tickers = [t.strip() for t in allowed_tickers_str.split(',') if t.strip()]
        
        whitelist_validation['details']['allowed_tickers'] = allowed_tickers
        whitelist_validation['details']['processed_tickers'] = tickers_processed
        
        unauthorized_tickers = [t for t in tickers_processed if t not in allowed_tickers]
        
        if unauthorized_tickers:
            whitelist_validation['status'] = 'failed'
            validation_results['errors'].append(
                f'Unauthorized tickers processed: {unauthorized_tickers}'
            )
            logger.error(f'Unauthorized tickers processed: {unauthorized_tickers}')
        else:
            logger.info(f'All processed tickers ({len(tickers_processed)}) are in whitelist')
        
        validation_results['validations']['ticker_whitelist'] = whitelist_validation
        
        # Determine overall validation status
        if validation_results['errors']:
            validation_results['status'] = 'failed'
        elif validation_results['warnings']:
            validation_results['status'] = 'warning'
        
        logger.info(f'Data quality validation completed with status: {validation_results["status"]}')
        return validation_results
        
    except Exception as e:
        logger.error(f'Data quality validation failed: {e}')
        validation_results['status'] = 'error'
        validation_results['errors'].append(f'Validation error: {str(e)}')
        return validation_results

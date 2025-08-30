'''
Airflow TaskFlow DAG with Dynamic Task Mapping for the stock data pipeline.

This module defines a daily ETL pipeline with dynamic task mapping that:
- Extracts historical prices from Yahoo Finance per ticker in parallel,
- Transforms them into technical metrics per ticker,
- Loads/upserts results into PostgreSQL.

Key improvements over the original DAG:
- Dynamic Task Mapping: Each ticker is processed in parallel tasks
- SQL Injection Protection: Proper validation of table names
- Better Error Handling: Individual ticker failures don't break the entire pipeline
- Scalability: Easily scales to handle more tickers without code changes

Key behaviors
-------------
- Incremental processing: determines a start/end window by inspecting the
  target table's latest stored date per ticker, with a configurable
  lookback fallback.
- Data passing via files: individual ticker data is stored as Parquet files in
  shared volume to avoid XCom size limits and preserve schema.
- Configuration via Airflow Variables:
  - TICKERS (comma-separated string)
  - INTERVAL (e.g., '1d', '1wk', '1mo')
  - LOOKBACK_DAYS (int)
  - TABLE_NAME (target table)
  - EMA_SPANS (JSON list, e.g. [9, 20, 50])
  - VOLATILITY_WINDOW (int)
- Database access via Airflow connections (with fallback to environment variables)
  using utils.get_db_config_with_fallback / utils.create_db_engine.
- Scheduling: '@daily', start_date = 2025-08-01, and catchup disabled
  at the DAG level (manual backfills can still be triggered if desired).

Tasks
-----
generate_ticker_tasks -> [extract_ticker_data, transform_ticker_data, load_ticker_data] (mapped)
-> validate_pipeline_execution
'''

from __future__ import annotations
import os
import glob
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy import text

# Make sure /opt/app is on the import path (mounted in docker-compose)
import sys
sys.path.append('/opt/app')

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable

# Import your ETL functions
from src.extract import fetch_stock_data
from src.transform import calculate_financial_metrics
from src.load import load_to_postgres
from src.utils import get_db_config_with_fallback, create_db_engine, validate_data_quality

import pandas as pd


# Default args for Airflow tasks
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def get_incremental_date_range_for_ticker(
    ticker: str,
    execution_date: datetime,
    lookback_days: int = 1,
    table_name: str = 'price_metrics'
) -> tuple[Optional[str], Optional[str]]:
    '''Get date range for incremental processing for a specific ticker.
    
    Strategy
    --------
    - Query the target table for MAX(date) for the specific ticker.
    - If no rows exist for this ticker, use [execution_date - lookback_days, execution_date].
    - Otherwise, use latest date + 1 day as start_date, and execution_date as end_date.
    - If start_date > end_date (no new data), return (None, None).
    - On any error, fall back to the lookback window.

    Parameters
    ----------
    ticker : str
        The ticker symbol to check.
    execution_date : datetime
        The date of the current execution.
    lookback_days : int, optional
        Number of days to look back if no existing data. Defaults to 1.
    table_name : str, optional
        Name of the target table. Defaults to 'price_metrics'.
    '''
    try:
        # Get database connection for checking existing data
        db_config = get_db_config_with_fallback()  # Use Airflow connections with fallback
        engine = create_db_engine(db_config)  # Use the connection config
        
        # Validate table name to prevent SQL injection (only allow alphanumeric and underscore)
        if not table_name.replace('_', '').isalnum():
            raise ValueError(f"Invalid table name: {table_name}")
            
        # Query to find the latest date for the specific ticker
        query = text(f'''
        SELECT MAX(date) as latest_date 
        FROM {table_name} 
        WHERE ticker = :ticker
        ''')
        
        with engine.connect() as conn:
            result = conn.execute(query, {'ticker': ticker})
            row = result.fetchone()
            latest_date = row[0] if row else None
        
        # Check if we need to backfill historical data to ensure sufficient data for technical indicators
        min_required_lookback = max(20, lookback_days)
        expected_start_date = execution_date - timedelta(days=min_required_lookback)
        
        if latest_date < expected_start_date.date():
            # We have a significant historical gap - backfill from expected start to ensure adequate data
            start_date = expected_start_date.strftime('%Y-%m-%d')
            end_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
            days_to_backfill = (execution_date.date() - expected_start_date.date()).days
            days_missing = (expected_start_date.date() - latest_date).days
            logging.info(f'Backfilling historical gap for {ticker}: latest data from {latest_date}, expected from {expected_start_date.date()}')
            logging.info(f'Fetching {days_to_backfill} days total (missing {days_missing} days of historical data)')
        else:
            # Normal incremental processing - we have sufficient historical data
            start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
            logging.info(f'Incremental processing for {ticker}: from {start_date} to {end_date} (sufficient historical data available)')
        
        # Ensure we don't process future dates - convert to dates for proper comparison
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        logging.info(f'Date range check for {ticker}: {start_date} to {end_date}')
        
        if start_date_obj > end_date_obj:
            # For manual runs or when no new data is available, 
            # allow reprocessing the most recent day if it's not too far in the future
            today = datetime.now().date()
            if end_date_obj >= today:
                # Allow reprocessing today's data for manual runs
                start_date = end_date
                logging.info(f'Allowing reprocessing for ticker {ticker} on {start_date}')
            else:
                logging.info(f'Skipping {ticker}: start_date ({start_date}) > end_date ({end_date}) and end_date is in the past')
                return None, None
            
        return start_date, end_date
        
    except Exception as e:
        logging.warning(f'Failed to determine incremental range for {ticker}, using lookback: {e}')
        start_date = (execution_date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        end_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
        return start_date, end_date


@task
def generate_ticker_tasks(**context) -> List[str]:
    '''Generate the list of tickers to process dynamically.
    
    This task reads the TICKERS variable and returns a list of individual
    ticker symbols that will be used for dynamic task mapping.
    
    Returns
    -------
    List[str]
        List of ticker symbols to process.
    '''
    logger = logging.getLogger(__name__)
    execution_date = context['execution_date']
    
    # Get configuration from Airflow Variables
    tickers = Variable.get('TICKERS', 'NVDA', deserialize_json=False)
    if isinstance(tickers, str):
        tickers_list = [t.strip() for t in tickers.split(',') if t.strip()]
    else:
        tickers_list = list(tickers)
    
    logger.info(f'Generated {len(tickers_list)} ticker tasks for execution date: {execution_date}')
    logger.info(f'Tickers: {tickers_list}')
    
    return tickers_list


@task
def extract_ticker_data(ticker: str, **context) -> Dict[str, Any]:
    '''Extract stock data for a single ticker with incremental processing support.
    
    This task is designed to be mapped dynamically across multiple tickers.
    Each instance processes one ticker independently.
    
    Parameters
    ----------
    ticker : str
        The ticker symbol to process.
        
    Returns
    -------
    Dict[str, Any]
        Metadata and data for the ticker, including status and serialized DataFrame.
    '''
    # Get execution context
    execution_date = context['execution_date']
    
    # Setup logging
    logger = logging.getLogger(__name__)
    logger.info(f'Starting extraction for ticker {ticker}, execution date: {execution_date}')
    
    # Get configuration from Airflow Variables
    interval = Variable.get('INTERVAL', '1d', deserialize_json=False)
    lookback_days = Variable.get('LOOKBACK_DAYS', 1, deserialize_json=False)
    table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
    
    # Determine date range for incremental processing
    start_date, end_date = get_incremental_date_range_for_ticker(
        ticker,
        execution_date, 
        lookback_days=int(lookback_days),
        table_name=table_name
    )
    
    if not start_date or not end_date:
        logger.info(f'No new data to process for ticker {ticker} on this execution date')
        logger.info(f'Date range result: start_date={start_date}, end_date={end_date}')
        logger.info(f'This usually means the ticker data is already up-to-date or execution_date is in the past')
        return {
            'status': 'skipped',
            'reason': 'no_new_data',
            'ticker': ticker,
            'execution_date': execution_date.isoformat(),
            'debug_info': {
                'start_date': start_date,
                'end_date': end_date,
                'explanation': 'Date range function returned None - data may be up-to-date'
            }
        }
    
    logger.info(f'Extracting data for ticker {ticker} from {start_date} to {end_date}')
    
    try:
        # Fetch the data for this ticker
        df = fetch_stock_data(
            tickers=[ticker], 
            start_date=start_date, 
            end_date=end_date, 
            interval=interval,
            flatten_columns=False
        )
        
        if df.empty:
            logger.warning(f'No data retrieved for ticker {ticker}')
            logger.warning(f'This could indicate:')
            logger.warning(f'  - Invalid ticker symbol: {ticker}')
            logger.warning(f'  - Delisted stock')
            logger.warning(f'  - No trading data for date range {start_date} to {end_date}')
            logger.warning(f'  - Network/API issues with Yahoo Finance')
            return {
                'status': 'skipped',
                'reason': 'no_data_retrieved',
                'ticker': ticker,
                'start_date': start_date,
                'end_date': end_date,
                'execution_date': execution_date.isoformat(),
                'troubleshooting': [
                    'Check if ticker symbol is correct',
                    'Verify ticker is not delisted',
                    'Check if ticker has data for the specified date range',
                    'Try running with a longer lookback period'
                ]
            }
        
        # Store DataFrame as Parquet file in shared volume
        # Parquet preserves DataFrame structure and is efficient for analytics data
        os.makedirs('/tmp/pipeline_data', exist_ok=True)
        file_path = f"/tmp/pipeline_data/dynamic_extract_{context['run_id']}_{ticker}.parquet"
        df.to_parquet(file_path)
        logger.info(f'Saved extracted data for {ticker} to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Return metadata and data
        metadata = {
            'status': 'success',
            'ticker': ticker,
            'start_date': start_date,
            'end_date': end_date,
            'interval': interval,
            'rows': len(df),
            'columns': list(df.columns),
            'date_range': {
                'start': df.index.min().isoformat() if not df.empty else None,
                'end': df.index.max().isoformat() if not df.empty else None
            },
            'execution_date': execution_date.isoformat(),
            'file_path': file_path  # Include file path
        }
        
        logger.info(f'Extraction completed successfully for {ticker}: {len(df)} rows')
        return metadata
        
    except Exception as e:
        logger.error(f'Extraction failed for ticker {ticker}: {e}')
        raise


@task
def transform_ticker_data(extract_result: Dict[str, Any], **context) -> Dict[str, Any]:
    '''Transform stock data for a single ticker with enhanced logging and error handling.

    This task is designed to be mapped dynamically across multiple tickers.
    Each instance processes one ticker independently.
    
    Parameters
    ----------
    extract_result : Dict[str, Any]
        Result from the extract task for this ticker.
        
    Returns
    -------
    Dict[str, Any]
        Metadata and transformed data for the ticker.
    '''
    logger = logging.getLogger(__name__)
    ticker = extract_result.get('ticker', 'UNKNOWN')
    
    if extract_result.get('status') == 'skipped':
        logger.info(f"Skipping transform for ticker {ticker}: {extract_result.get('reason')}")
        return extract_result
    
    logger.info(f"Starting transformation for ticker {ticker}, {extract_result['rows']} rows")
    
    try:
        # Get the extracted data from file
        extract_file_path = extract_result.get('file_path')
        if not extract_file_path or not os.path.exists(extract_file_path):
            raise ValueError(f"Extracted data file not found for ticker {ticker}: {extract_file_path}")
        
        # Read DataFrame from Parquet file
        df = pd.read_parquet(extract_file_path)
        logger.info(f'Loaded extracted data for {ticker} from {extract_file_path} ({os.path.getsize(extract_file_path)} bytes)')
        
        # Debug: Log DataFrame structure
        logger.info(f'DataFrame shape for {ticker}: {df.shape}')
        logger.info(f'DataFrame columns for {ticker}: {df.columns.tolist()}')
        
        # Get transformation parameters from Variables
        ema_spans = Variable.get('EMA_SPANS', [9, 20, 50], deserialize_json=True)
        volatility_window = Variable.get('VOLATILITY_WINDOW', 21, deserialize_json=False)
        
        logger.info(f'Applying transformations for {ticker}: EMA spans {ema_spans}, volatility window {volatility_window}')
        
        # Transform the data
        metrics = calculate_financial_metrics(
            df, 
            ema_spans=ema_spans, 
            volatility_window=int(volatility_window),
            validate_columns=True
        )
        
        if metrics.empty:
            logger.warning(f'No metrics calculated for ticker {ticker}')
            logger.warning(f'Original data shape for {ticker}: {df.shape}')
            logger.warning(f'This often happens with new tickers that have insufficient data for technical indicators')
            return {
                'status': 'skipped',
                'reason': 'no_metrics_calculated',
                'ticker': ticker,
                'execution_date': extract_result['execution_date'],
                'debug_info': {
                    'original_data_shape': df.shape,
                    'original_data_columns': df.columns.tolist() if not df.empty else []
                }
            }
        
        # Store the transformed data as Parquet file in shared volume
        # Parquet preserves DataFrame structure and handles dates properly
        file_path = f"/tmp/pipeline_data/dynamic_transform_{context['run_id']}_{ticker}.parquet"
        metrics.to_parquet(file_path, index=False)
        logger.info(f'Saved transformed data for {ticker} to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Clean up the input file
        if os.path.exists(extract_file_path):
            os.unlink(extract_file_path)
            logger.info(f'Cleaned up input file: {extract_file_path}')
        
        # Return metadata for downstream tasks
        transform_metadata = {
            'status': 'success',
            'ticker': ticker,
            'rows': len(metrics),
            'columns': list(metrics.columns),
            'date_range': extract_result['date_range'],
            'execution_date': extract_result['execution_date'],
            'transformation_params': {
                'ema_spans': ema_spans,
                'volatility_window': volatility_window
            },
            'file_path': file_path  # Include file path
        }
        
        logger.info(f'Transformation completed successfully for {ticker}: {len(metrics)} rows processed')
        return transform_metadata
        
    except Exception as e:
        logger.error(f'Transformation failed for ticker {ticker}: {e}')
        raise


@task
def load_ticker_data(transform_result: Dict[str, Any], **context) -> Dict[str, Any]:
    '''Load transformed data for a single ticker into PostgreSQL with connection management.
    
    This task is designed to be mapped dynamically across multiple tickers.
    Each instance processes one ticker independently.
    
    Parameters
    ----------
    transform_result : Dict[str, Any]
        Result from the transform task for this ticker.
        
    Returns
    -------
    Dict[str, Any]
        Metadata about the load operation for this ticker.
    '''
    logger = logging.getLogger(__name__)
    ticker = transform_result.get('ticker', 'UNKNOWN')
    
    if transform_result.get('status') == 'skipped':
        logger.info(f"Skipping load for ticker {ticker}: {transform_result.get('reason')}")
        return transform_result
    
    logger.info(f"Starting load for ticker {ticker}, {transform_result['rows']} rows")
    
    try:
        # Get the transformed data from file
        transform_file_path = transform_result.get('file_path')
        if not transform_file_path or not os.path.exists(transform_file_path):
            raise ValueError(f"Transformed data file not found for ticker {ticker}: {transform_file_path}")
        
        # Read DataFrame from Parquet file
        metrics = pd.read_parquet(transform_file_path)
        logger.info(f'Loaded transformed data for {ticker} from {transform_file_path} ({os.path.getsize(transform_file_path)} bytes)')
        
        # Clean up the transformed data file
        if os.path.exists(transform_file_path):
            os.unlink(transform_file_path)
            logger.info(f'Cleaned up transformed file: {transform_file_path}')
        
        # Debug: Log DataFrame structure
        logger.info(f'Metrics DataFrame shape for {ticker}: {metrics.shape}')
        logger.info(f'Metrics DataFrame columns for {ticker}: {metrics.columns.tolist()}')
        
        # Get configuration from Airflow Variables
        table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
        
        # Get database connection configuration
        db_config = get_db_config_with_fallback()
        
        # Debug: Log database configuration (without password)
        debug_config = {k: v for k, v in db_config.items() if k != 'password'}
        logger.info(f'Database configuration for {ticker}: {debug_config}')
        
        logger.info(f'Loading data for ticker {ticker} into table: {table_name}')
        
        # Load to database
        load_to_postgres(metrics, table_name=table_name, db_config=db_config)
        
        # Return load metadata
        load_metadata = {
            'status': 'success',
            'ticker': ticker,
            'table_name': table_name,
            'rows_loaded': len(metrics),
            'date_range': transform_result['date_range'],
            'execution_date': transform_result['execution_date'],
            'load_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f'Load completed successfully for ticker {ticker}: {len(metrics)} rows loaded into {table_name}')
        return load_metadata
        
    except Exception as e:
        logger.error(f'Load failed for ticker {ticker}: {e}')
        raise


@task
def cleanup_temp_files(**context) -> dict:
    '''Clean up temporary pipeline files for dynamic DAG.
    
    Behavior
    --------
    - Removes Parquet files older than 7 days from /tmp/pipeline_data
    - Focuses on dynamic DAG files (prefix: dynamic_)
    - Logs cleanup activity and file sizes
    
    Returns
    -------
    dict
        Metadata with files_cleaned count, size_cleaned, and timestamps.
    '''
    logger = logging.getLogger(__name__)
    
    cleanup_dir = '/tmp/pipeline_data'
    retention_days = 7  # Keep last week's files
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    
    files_cleaned = 0
    total_size_cleaned = 0
    
    try:
        if not os.path.exists(cleanup_dir):
            logger.info(f'Cleanup directory {cleanup_dir} does not exist, nothing to clean')
        else:
            # Find old dynamic DAG Parquet files
            dynamic_files = glob.glob(f'{cleanup_dir}/dynamic_*.parquet')
            
            for file_path in dynamic_files:
                try:
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_time < cutoff_time:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        files_cleaned += 1
                        total_size_cleaned += file_size
                        logger.info(f'Cleaned up: {file_path} ({file_size} bytes)')
                except Exception as e:
                    logger.warning(f'Failed to clean up {file_path}: {e}')
        
        cleanup_metadata = {
            'status': 'success',
            'files_cleaned': files_cleaned,
            'size_cleaned_mb': round(total_size_cleaned / 1024 / 1024, 2),
            'retention_days': retention_days,
            'execution_date': context['execution_date'].isoformat(),
            'cleanup_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f'Dynamic DAG cleanup completed: {files_cleaned} files removed, {cleanup_metadata["size_cleaned_mb"]} MB freed')
        return cleanup_metadata
        
    except Exception as e:
        logger.error(f'Dynamic DAG cleanup failed: {e}')
        raise


@task
def validate_pipeline_execution(load_results: List[Dict[str, Any]], **context) -> Dict[str, Any]:
    '''Validate that the pipeline executed successfully across all tickers.
    
    Performs both pipeline-level validation and data quality validation:
    - Pipeline execution status across all tickers
    - Row count validation (> 0 for each ticker)
    - Date continuity validation (no gaps in date sequences)
    - Ticker whitelist validation (all processed tickers are allowed)
    
    Parameters
    ----------
    load_results : List[Dict[str, Any]]
        List of results from all load tasks.
        
    Returns
    -------
    Dict[str, Any]
        Overall validation result for the pipeline execution.
    '''
    logger = logging.getLogger(__name__)
    
    logger.info('Validating dynamic pipeline execution')
    
    try:
        # Get configuration for data quality validation
        table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
        lookback_days = Variable.get('LOOKBACK_DAYS', 30, deserialize_json=False)
        db_config = get_db_config_with_fallback()
        
        # Analyze results from all ticker tasks
        total_tickers = len(load_results)
        successful_tickers = []
        skipped_tickers = []
        failed_tickers = []
        total_rows_loaded = 0
        
        for result in load_results:
            ticker = result.get('ticker', 'UNKNOWN')
            status = result.get('status', 'unknown')
            
            if status == 'success':
                successful_tickers.append(ticker)
                total_rows_loaded += result.get('rows_loaded', 0)
            elif status == 'skipped':
                skipped_tickers.append(ticker)
            else:
                failed_tickers.append(ticker)
        
        # Determine overall status
        if failed_tickers:
            overall_status = 'partial_failure'
        elif skipped_tickers and not successful_tickers:
            overall_status = 'all_skipped'
        elif skipped_tickers:
            overall_status = 'partial_success'
        else:
            overall_status = 'success'
        
        # Pipeline-level validation result
        validation_result = {
            'status': overall_status,
            'execution_date': context['execution_date'].isoformat(),
            'validation_timestamp': datetime.now().isoformat(),
            'total_tickers': total_tickers,
            'successful_tickers': successful_tickers,
            'skipped_tickers': skipped_tickers,
            'failed_tickers': failed_tickers,
            'total_rows_loaded': total_rows_loaded,
            'success_rate': len(successful_tickers) / total_tickers if total_tickers > 0 else 0
        }
        
        # Log pipeline summary
        logger.info(f'Pipeline validation complete:')
        logger.info(f'  Total tickers: {total_tickers}')
        logger.info(f'  Successful: {len(successful_tickers)}')
        logger.info(f'  Skipped: {len(skipped_tickers)}')
        logger.info(f'  Failed: {len(failed_tickers)}')
        logger.info(f'  Total rows loaded: {total_rows_loaded}')
        logger.info(f'  Success rate: {validation_result["success_rate"]:.2%}')
        
        # Perform data quality validation if we have successful tickers
        if successful_tickers:
            logger.info('Performing data quality validation')
            data_quality_result = validate_data_quality(
                db_config=db_config,
                table_name=table_name,
                tickers_processed=successful_tickers,
                execution_date=context['execution_date'],
                lookback_days=lookback_days
            )
            
            # Merge data quality results into validation result
            validation_result['data_quality'] = data_quality_result
            
            # Update overall status based on data quality
            if data_quality_result['status'] == 'failed':
                if validation_result['status'] == 'success':
                    validation_result['status'] = 'data_quality_failed'
                elif validation_result['status'] == 'partial_success':
                    validation_result['status'] = 'partial_success_with_data_quality_issues'
            
            # Log data quality summary
            if data_quality_result['errors']:
                logger.error(f'Data quality errors: {data_quality_result["errors"]}')
            if data_quality_result['warnings']:
                logger.warning(f'Data quality warnings: {data_quality_result["warnings"]}')
        else:
            logger.info('No successful tickers to validate for data quality')
            validation_result['data_quality'] = {
                'status': 'skipped',
                'reason': 'No successful tickers to validate'
            }
        
        return validation_result
        
    except Exception as e:
        logger.error(f'Validation failed: {e}')
        raise


# Define the DAG using TaskFlow API with Dynamic Task Mapping
@dag(
    dag_id='stock_pipeline_dynamic_dag',
    description='Enhanced stock pipeline with dynamic task mapping, SQL injection protection, and parallel ticker processing',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 1),  # Start from August 1, 2025
    catchup=False,  # Disable catchup to prevent hundreds of API calls
    tags=['stocks', 'etl', 'taskflow', 'dynamic-mapping', 'parallel'],
    max_active_runs=3,  # Limit concurrent runs
    doc_md='''
    ## Dynamic Stock Pipeline DAG
    
    This DAG extracts, transforms, and loads stock price data with the following enhanced features:
    
    - **Dynamic Task Mapping**: Each ticker is processed in parallel tasks for better scalability
    - **SQL Injection Protection**: Proper validation of table names and parameterized queries
    - **Incremental Processing**: Only processes new data since last execution per ticker
    - **Airflow Connections**: Uses managed database connections
    - **Backfill Support**: Can reprocess historical data
    - **Individual Error Handling**: Ticker failures don't break the entire pipeline
    - **Comprehensive Logging**: Detailed logging for monitoring and debugging
    
    ### Configuration Variables:
    - `TICKERS`: Comma-separated list of stock tickers
    - `INTERVAL`: Data interval (1d, 1wk, 1mo)
    - `LOOKBACK_DAYS`: Days to look back if no existing data
    - `TABLE_NAME`: Target PostgreSQL table
    - `EMA_SPANS`: List of EMA periods for technical indicators
    - `VOLATILITY_WINDOW`: Rolling window for volatility calculation
    
    ### Airflow Connections:
    - `postgres_default`: PostgreSQL database connection
    
    ### Task Structure:
    ```
    generate_ticker_tasks
    ↓
    [extract_ticker_data] (mapped)
    ↓
    [transform_ticker_data] (mapped)
    ↓
    [load_ticker_data] (mapped)
    ↓
    cleanup_temp_files
    ↓
    validate_pipeline_execution
    ```
    ''',
)
def stock_pipeline_dynamic_dag():
    '''Enhanced stock pipeline DAG with dynamic task mapping and security improvements.'''
    
    # Generate ticker tasks dynamically
    ticker_list = generate_ticker_tasks()
    
    # Map extract, transform, and load tasks across tickers
    extract_results = extract_ticker_data.expand(ticker=ticker_list)
    transform_results = transform_ticker_data.expand(extract_result=extract_results)
    load_results = load_ticker_data.expand(transform_result=transform_results)
    
    # Cleanup temporary files
    cleanup_result = cleanup_temp_files()
    
    # Validate overall pipeline execution
    validation_result = validate_pipeline_execution(load_results)
    
    # Set dependencies
    load_results >> cleanup_result >> validation_result
    
    return {
        'ticker_list': ticker_list,
        'extract_results': extract_results,
        'transform_results': transform_results,
        'load_results': load_results,
        'cleanup_result': cleanup_result,
        'validation_result': validation_result
    }


# Create the DAG instance
dag = stock_pipeline_dynamic_dag()

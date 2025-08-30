'''
Airflow TaskFlow DAG for the stock data pipeline.

This module defines a daily ETL pipeline that:
- Extracts historical prices from Yahoo Finance (via yfinance),
- Transforms them into technical metrics,
- Loads/upserts results into PostgreSQL.

Key behaviors
-------------
- Incremental processing: determines a start/end window by inspecting the
  target table's latest stored date per ticker, with a configurable
  lookback fallback.
- Data passing via files: pandas DataFrames are stored as Parquet files in
  shared volume to avoid XCom size limits and preserve schema (multi-index, dtypes).
- Configuration via Airflow Variables:
  - TICKERS (comma-separated string)
  - INTERVAL (fx, '1d', '1wk', '1mo')
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
extract_stock_data -> transform_stock_data -> load_stock_data
-> cleanup_temp_files -> validate_pipeline_execution
'''

from __future__ import annotations
import os
import glob
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
   
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
from sqlalchemy import text

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


def get_incremental_date_range(
    execution_date: datetime,
    lookback_days: int = 1,
    table_name: str = 'price_metrics'
) -> tuple[Optional[str], Optional[str]]:
    '''Get date range for incremental processing based on existing data.
    
    Strategy
    --------
    - Query the target table for MAX(date) per ticker.
    - If no rows exist, use [execution_date - lookback_days, execution_date].
    - Otherwise, use the earliest of the latest dates across tickers + 1 day
      as start_date, and execution_date as end_date.
    - If start_date > end_date (no new data), return (None, None).
    - On any error, fall back to the lookback window.

    Parameters
    ----------
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
            
        # Query to find the latest date for each ticker
        query = text(f'''
        SELECT ticker, MAX(date) as latest_date 
        FROM {table_name} 
        GROUP BY ticker
        ''')
        
        with engine.connect() as conn:
            result = conn.execute(query)
            latest_dates = {row[0]: row[1] for row in result}
        
        # If no existing data, use lookback period (more generous for new tickers)
        if not latest_dates:
            # For new tickers, use a minimum of 30 days or the configured lookback_days, whichever is larger
            # This ensures we get enough data for technical indicators
            min_lookback_for_new_ticker = max(30, lookback_days)
            start_date = (execution_date - timedelta(days=min_lookback_for_new_ticker)).strftime('%Y-%m-%d')
            end_date = execution_date.strftime('%Y-%m-%d')
            logging.info(f'New tickers detected: using {min_lookback_for_new_ticker} day lookback ({start_date} to {end_date})')
            return start_date, end_date
        
        # Use the earliest latest date across all tickers for consistency
        earliest_latest = min(latest_dates.values())
        start_date = (earliest_latest + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = execution_date.strftime('%Y-%m-%d')
        
        # Ensure we don't process future dates
        if start_date > end_date:
            # For manual runs or when no new data is available, 
            # allow reprocessing the most recent day if it's not too far in the future
            today = datetime.now().date().strftime('%Y-%m-%d')
            if end_date >= today:
                # Allow reprocessing today's data for manual runs
                start_date = end_date
                logging.info(f'Allowing reprocessing on {start_date}')
            else:
                return None, None
            
        return start_date, end_date
        
    except Exception as e:
        logging.warning(f'Failed to determine incremental range, using lookback: {e}')
        start_date = (execution_date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        end_date = execution_date.strftime('%Y-%m-%d')
        return start_date, end_date


@task
def extract_stock_data(**context) -> dict:
    '''Extract stock data with incremental processing support.
    
    Behavior
    --------
    - Reads Variables: TICKERS, INTERVAL, LOOKBACK_DAYS, TABLE_NAME.
    - Calls get_incremental_date_range() to determine [start_date, end_date].
    - If no new data is needed, returns a 'skipped' metadata dict.
    - Otherwise calls src.extract.fetch_stock_data(...) to fetch prices.
    - Saves the returned DataFrame as Parquet file in shared volume.
      Returns file path in metadata for downstream tasks.

    Returns
    -------
    dict
        Metadata including status ('success' or 'skipped'), tickers, date_range,
        interval, and basic shape info.
    '''
    # Get execution context
    execution_date = context['execution_date']
    task_instance = context['task_instance']
    
    # Setup logging
    logger = logging.getLogger(__name__)
    logger.info(f'Starting extraction for execution date: {execution_date}')
    
    # Get configuration from Airflow Variables or environment
    tickers = Variable.get('TICKERS', 'NVDA', deserialize_json=False)
    if isinstance(tickers, str):
        tickers = [t.strip() for t in tickers.split(',') if t.strip()]
    
    interval = Variable.get('INTERVAL', '1d', deserialize_json=False)
    lookback_days = Variable.get('LOOKBACK_DAYS', 1, deserialize_json=False)
    table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
    
    # Determine date range for incremental processing
    start_date, end_date = get_incremental_date_range(
        execution_date, 
        lookback_days=int(lookback_days),
        table_name=table_name
    )
    
    if not start_date or not end_date:
        logger.info('No new data to process for this execution date')
        return {
            'status': 'skipped',
            'reason': 'no_new_data',
            'tickers': tickers,
            'execution_date': execution_date.isoformat()
        }
    
    logger.info(f'Extracting data for tickers: {tickers} from {start_date} to {end_date}')
    
    try:
        # Fetch the data
        df = fetch_stock_data(
            tickers=tickers, 
            start_date=start_date, 
            end_date=end_date, 
            interval=interval
        )
        
        if df.empty:
            logger.warning('No data retrieved for the specified parameters')
            return {
                'status': 'skipped',
                'reason': 'no_data_retrieved',
                'tickers': tickers,
                'start_date': start_date,
                'end_date': end_date,
                'execution_date': execution_date.isoformat()
            }
        
        # Store DataFrame as Parquet file in shared volume
        # Parquet preserves DataFrame structure including multi-index columns and dtypes
        os.makedirs('/tmp/pipeline_data', exist_ok=True)
        file_path = f"/tmp/pipeline_data/extract_{context['run_id']}_{context['task_instance'].task_id}.parquet"
        df.to_parquet(file_path)
        logger.info(f'Saved extracted data to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Return metadata for downstream tasks
        metadata = {
            'status': 'success',
            'tickers': tickers,
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
            'file_path': file_path
        }
        
        logger.info(f'Extraction completed successfully: {len(df)} rows for {len(tickers)} tickers')
        task_instance.xcom_push(key='extract_metadata', value=metadata)
        return metadata
        
    except Exception as e:
        logger.error(f'Extraction failed: {e}')
        raise


@task
def transform_stock_data(extract_metadata: dict, **context) -> dict:
    '''Transform stock data with enhanced logging and error handling.

    Behavior
    --------
    - If extraction was skipped, returns the same 'skipped' metadata.
    - Reads DataFrame from Parquet file specified in extract metadata.
    - Reads Variables: EMA_SPANS (JSON list), VOLATILITY_WINDOW (int).
    - Calls src.transform.calculate_financial_metrics(...) to compute returns,
      volatility, and EMAs.
    - Saves the resulting metrics DataFrame as Parquet file in shared volume.
      Returns file path in metadata for downstream tasks.

    Returns
    -------
    dict
        Metadata including status ('success' or 'skipped'), rows, columns,
        tickers, date_range, and transformation parameters.
    '''
    task_instance = context['task_instance']
    logger = logging.getLogger(__name__)
    
    if extract_metadata.get('status') == 'skipped':
        logger.info(f"Skipping transform: {extract_metadata.get('reason')}")
        return extract_metadata
    
    logger.info(f"Starting transformation for {extract_metadata['rows']} rows")
    
    try:
        # Get the extracted data from file
        extract_file_path = extract_metadata.get('file_path')
        if not extract_file_path or not os.path.exists(extract_file_path):
            raise ValueError(f"Extracted data file not found: {extract_file_path}")
        
        # Read DataFrame from Parquet file
        df = pd.read_parquet(extract_file_path)
        logger.info(f'Loaded extracted data from {extract_file_path} ({os.path.getsize(extract_file_path)} bytes)')
        
        # Debug: Log DataFrame structure and columns
        logger.info(f'DataFrame shape: {df.shape}')
        logger.info(f'DataFrame columns: {df.columns.tolist()}')
        logger.info(f'DataFrame index type: {type(df.index)}')
        if hasattr(df.columns, 'levels'):
            logger.info(f'Column levels: {df.columns.levels}')
            logger.info(f'Column names: {df.columns.names}')
        
        # Additional debugging for column structure
        if len(df.columns) > 0:
            logger.info(f'First few column names: {df.columns[:10].tolist()}')
            if hasattr(df.columns, 'get_level_values'):
                logger.info(f'Level 0 values: {df.columns.get_level_values(0).unique().tolist()}')
                if len(df.columns.levels) > 1:
                    logger.info(f'Level 1 values: {df.columns.get_level_values(1).unique().tolist()}')
        
        # Get transformation parameters from Variables
        ema_spans = Variable.get('EMA_SPANS', [9, 20, 50], deserialize_json=True)
        volatility_window = Variable.get('VOLATILITY_WINDOW', 21, deserialize_json=False)
        
        logger.info(f'Applying transformations: EMA spans {ema_spans}, volatility window {volatility_window}')
        
        # Transform the data
        metrics = calculate_financial_metrics(
            df, 
            ema_spans=ema_spans, 
            volatility_window=int(volatility_window)
        )
        
        # Store the transformed data as Parquet file in shared volume
        # Parquet preserves DataFrame structure and is efficient for analytics data
        file_path = f"/tmp/pipeline_data/transform_{context['run_id']}_{context['task_instance'].task_id}.parquet"
        metrics.to_parquet(file_path)
        logger.info(f'Saved transformed data to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Return metadata for downstream tasks
        transform_metadata = {
            'status': 'success',
            'rows': len(metrics),
            'columns': list(metrics.columns),
            'tickers': extract_metadata['tickers'],
            'date_range': extract_metadata['date_range'],
            'execution_date': extract_metadata['execution_date'],
            'file_path': file_path,
            'transformation_params': {
                'ema_spans': ema_spans,
                'volatility_window': volatility_window
            }
        }
        
        logger.info(f'Transformation completed successfully: {len(metrics)} rows processed')
        task_instance.xcom_push(key='transform_metadata', value=transform_metadata)
        return transform_metadata
        
    except Exception as e:
        logger.error(f'Transformation failed: {e}')
        raise


@task
def load_stock_data(transform_metadata: dict, **context) -> dict:
    '''Load transformed data into PostgreSQL with connection management.
    
     Behavior
    --------
    - If upstream was skipped, returns the same 'skipped' metadata.
    - Reads DataFrame from Parquet file specified in transform metadata.
    - Reads Variable: TABLE_NAME to select the target table.
    - Resolves DB connection via utils.get_db_config() (environment-based).
    - Calls src.load.load_to_postgres(...) to upsert rows.
    - Pushes 'load_metadata' with counts and timing.

    Returns
    -------
    dict
        Metadata including status, table_name, rows_loaded, tickers, date_range,
        execution_date, and a load timestamp.
    '''
    task_instance = context['task_instance']
    logger = logging.getLogger(__name__)
    
    if transform_metadata.get('status') == 'skipped':
        logger.info(f"Skipping load: {transform_metadata.get('reason')}")
        return transform_metadata
    
    logger.info(f"Starting load for {transform_metadata['rows']} rows")
    
    try:
        # Get the transformed data from file
        transform_file_path = transform_metadata.get('file_path')
        if not transform_file_path or not os.path.exists(transform_file_path):
            raise ValueError(f"Transformed data file not found: {transform_file_path}")
        
        # Read DataFrame from Parquet file
        metrics = pd.read_parquet(transform_file_path)
        logger.info(f'Loaded transformed data from {transform_file_path} ({os.path.getsize(transform_file_path)} bytes)')
        
        # Debug: Log DataFrame structure and columns
        logger.info(f'Metrics DataFrame shape: {metrics.shape}')
        logger.info(f'Metrics DataFrame columns: {metrics.columns.tolist()}')
        
        # Get configuration from Airflow Variables
        table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
        
        # Get database connection configuration
        db_config = get_db_config_with_fallback()
        
        # Debug: Log database configuration (without password)
        debug_config = {k: v for k, v in db_config.items() if k != 'password'}
        logger.info(f'Database configuration: {debug_config}')
        
        logger.info(f'Loading data into table: {table_name}')
        
        # Load to database
        load_to_postgres(metrics, table_name=table_name, db_config=db_config)
        
        # Return load metadata
        load_metadata = {
            'status': 'success',
            'table_name': table_name,
            'rows_loaded': len(metrics),
            'tickers': transform_metadata['tickers'],
            'date_range': transform_metadata['date_range'],
            'execution_date': transform_metadata['execution_date'],
            'load_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f'Load completed successfully: {len(metrics)} rows loaded into {table_name}')
        task_instance.xcom_push(key='load_metadata', value=load_metadata)
        return load_metadata
        
    except Exception as e:
        logger.error(f'Load failed: {e}')
        raise


@task
def cleanup_temp_files(**context) -> dict:
    '''Clean up temporary pipeline files older than retention period.
    
    Behavior
    --------
    - Removes Parquet files older than 7 days from /tmp/pipeline_data
    - Logs cleanup activity and file sizes
    - Returns metadata about cleanup operation
    
    Returns
    -------
    dict
        Metadata with files_cleaned count, size_cleaned, and timestamps.
    '''
    task_instance = context['task_instance']
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
            # Find old Parquet files
            all_files = glob.glob(f'{cleanup_dir}/*.parquet')
            
            for file_path in all_files:
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
        
        logger.info(f'Cleanup completed: {files_cleaned} files removed, {cleanup_metadata["size_cleaned_mb"]} MB freed')
        task_instance.xcom_push(key='cleanup_metadata', value=cleanup_metadata)
        return cleanup_metadata
        
    except Exception as e:
        logger.error(f'Cleanup failed: {e}')
        raise


@task
def validate_pipeline_execution(**context) -> dict:
    '''Validate that the pipeline executed successfully.
    
    Performs both pipeline-level validation and data quality validation:
    - Pipeline execution status across all tasks
    - Row count validation (> 0 for each ticker)
    - Date continuity validation (no gaps in date sequences)
    - Ticker whitelist validation (all processed tickers are allowed)
    '''
    task_instance = context['task_instance']
    logger = logging.getLogger(__name__)
    
    logger.info('Validating pipeline execution')
    
    try:
        # Get configuration for data quality validation
        table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
        lookback_days = Variable.get('LOOKBACK_DAYS', 30, deserialize_json=False)
        db_config = get_db_config_with_fallback()
        
        # Get metadata from all previous tasks
        extract_meta = task_instance.xcom_pull(task_ids='extract_stock_data', key='extract_metadata')
        transform_meta = task_instance.xcom_pull(task_ids='transform_stock_data', key='transform_metadata')
        load_meta = task_instance.xcom_pull(task_ids='load_stock_data', key='load_metadata')
        cleanup_meta = task_instance.xcom_pull(task_ids='cleanup_temp_files', key='cleanup_metadata')
        
        # Check if any task was skipped
        skipped_tasks = []
        if extract_meta and extract_meta.get('status') == 'skipped':
            skipped_tasks.append('extract')
        if transform_meta and transform_meta.get('status') == 'skipped':
            skipped_tasks.append('transform')
        if load_meta and load_meta.get('status') == 'skipped':
            skipped_tasks.append('load')
        
        # Get tickers processed
        tickers_processed = extract_meta.get('tickers', []) if extract_meta else []
        
        # Pipeline-level validation result
        validation_result = {
            'status': 'success',
            'execution_date': context['execution_date'].isoformat(),
            'validation_timestamp': datetime.now().isoformat(),
            'tasks_executed': {
                'extract': extract_meta.get('status') if extract_meta else 'unknown',
                'transform': transform_meta.get('status') if transform_meta else 'unknown',
                'load': load_meta.get('status') if load_meta else 'unknown',
                'cleanup': cleanup_meta.get('status') if cleanup_meta else 'unknown'
            },
            'skipped_tasks': skipped_tasks,
            'total_rows_processed': load_meta.get('rows_loaded', 0) if load_meta else 0,
            'tickers_processed': tickers_processed
        }
        
        if skipped_tasks:
            validation_result['status'] = 'partial_success'
            logger.info(f'Pipeline completed with skipped tasks: {skipped_tasks}')
        else:
            logger.info('Pipeline completed successfully with all tasks executed')
        
        # Perform data quality validation if we have successful load and tickers
        if load_meta and load_meta.get('status') == 'success' and tickers_processed:
            logger.info('Performing data quality validation')
            data_quality_result = validate_data_quality(
                db_config=db_config,
                table_name=table_name,
                tickers_processed=tickers_processed,
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
            logger.info('No successful load or tickers to validate for data quality')
            validation_result['data_quality'] = {
                'status': 'skipped',
                'reason': 'No successful load or tickers to validate'
            }
        
        task_instance.xcom_push(key='validation_result', value=validation_result)
        return validation_result
        
    except Exception as e:
        logger.error(f'Validation failed: {e}')
        raise


# Define the DAG using TaskFlow API
@dag(
    dag_id='stock_pipeline_dag',
    description='Enhanced stock pipeline with incremental processing, connections, and backfill support',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 1),  # Start from August 1, 2025
    catchup=False,  # Disable catchup to prevent hundreds of API calls in super fast pace
    tags=['stocks', 'etl', 'taskflow', 'incremental'],
    max_active_runs=3,  # Limit concurrent runs
    is_paused_upon_creation=True,  # Start DAG in paused state
    doc_md='''
    ## Stock Pipeline DAG
    
    This DAG extracts, transforms, and loads stock price data with the following features:
    
    - Incremental Processing: Only processes new data since last execution
    - Airflow Connections: Uses managed database connections
    - Backfill Support: Can reprocess historical data
    - Proper Logging: Comprehensive logging for monitoring and debugging
    - Error Handling: Graceful handling of failures with cleanup
    
    ### Configuration Variables:
    - 'TICKERS': Comma-separated list of stock tickers
    - 'INTERVAL': Data interval (1d, 1wk, 1mo)
    - 'LOOKBACK_DAYS': Days to look back if no existing data
    - 'TABLE_NAME': Target PostgreSQL table
    - 'EMA_SPANS': List of EMA periods for technical indicators
    - 'VOLATILITY_WINDOW': Rolling window for volatility calculation
    
    ### Airflow Connections:
    - 'postgres_default': PostgreSQL database connection
    ''',
)
def stock_pipeline_dag():
    '''Enhanced stock pipeline DAG with incremental processing and production features.'''
    
    # Define task dependencies
    extract_result = extract_stock_data()
    transform_result = transform_stock_data(extract_result)
    load_result = load_stock_data(transform_result)
    cleanup_result = cleanup_temp_files()
    validation_result = validate_pipeline_execution()
    
    # Set up task dependencies
    extract_result >> transform_result >> load_result >> cleanup_result >> validation_result
    
    return {
        'extract': extract_result,
        'transform': transform_result,
        'load': load_result,
        'cleanup': cleanup_result,
        'validation': validation_result
    }


# Create the DAG instance
dag = stock_pipeline_dag()



from __future__ import annotations
import os
import glob
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

# Make sure /opt/app is on the import path (mounted in docker-compose)
import sys
sys.path.append('/opt/app')

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Import your ETL functions
from src.extract import fetch_stock_data
from src.transform import calculate_financial_metrics
from src.load import load_to_postgres
from src.utils import get_db_config_with_fallback, create_db_engine, validate_data_quality

import pandas as pd
import time


# Default args for bootstrap DAG
BOOTSTRAP_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,  # No catchup for bootstrap
}


def bootstrap_extract_single_ticker(ticker: str, **context) -> dict:
    '''Extract historical stock data for a single ticker.'''
    execution_date = context['execution_date']
    task_instance = context['task_instance']
    
    logger = logging.getLogger(__name__)
    logger.info(f'Starting bootstrap extraction for ticker: {ticker}, execution date: {execution_date}')
    
    # Get configuration from Airflow Variables
    interval = Variable.get('INTERVAL', '1d', deserialize_json=False)
    
    # Fixed date range: always from 2019-01-01 to yesterday
    start_date = datetime(2019, 1, 1).date()
    end_date = (datetime.now() - timedelta(days=1)).date()
    logger.info(f'Using fixed date range: {start_date} to {end_date}')
    
    logger.info(f'Bootstrap extracting data for ticker: {ticker} from {start_date} to {end_date}')
    
    try:
        # Add small delay to prevent API rate limiting (shorter since we're processing one ticker)
        time.sleep(1)
        
        # Fetch the data for single ticker
        df = fetch_stock_data(
            tickers=[ticker], 
            start_date=start_date.strftime('%Y-%m-%d'), 
            end_date=end_date.strftime('%Y-%m-%d'), 
            interval=interval
        )
        
        if df.empty:
            logger.warning(f'No data retrieved for ticker: {ticker}')
            return {
                'status': 'failed',
                'reason': 'no_data_retrieved',
                'ticker': ticker,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'execution_date': execution_date.isoformat()
            }
        
        # Store DataFrame as Parquet file in shared volume
        # Parquet preserves DataFrame structure and is efficient for analytics data
        os.makedirs('/tmp/pipeline_data', exist_ok=True)
        file_path = f"/tmp/pipeline_data/bootstrap_extract_{context['run_id']}_{ticker}.parquet"
        df.to_parquet(file_path)
        logger.info(f'Saved bootstrap extracted data for {ticker} to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Return metadata with file path
        metadata = {
            'status': 'success',
            'ticker': ticker,
            'file_path': file_path,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'interval': interval,
            'rows': len(df),
            'columns': list(df.columns),
            'date_range': {
                'start': df.index.min().isoformat() if not df.empty else None,
                'end': df.index.max().isoformat() if not df.empty else None
            },
            'execution_date': execution_date.isoformat()
        }
        
        logger.info(f'Bootstrap extraction completed for {ticker}: {len(df)} rows')
        return metadata
        
    except Exception as e:
        logger.error(f'Bootstrap extraction failed for {ticker}: {e}')
        raise


def bootstrap_transform_single_ticker(extract_metadata: dict, **context) -> dict:
    '''Transform bootstrap stock data for a single ticker.'''
    logger = logging.getLogger(__name__)
    
    if extract_metadata.get('status') == 'failed':
        logger.info(f"Skipping transform for {extract_metadata.get('ticker')}: {extract_metadata.get('reason')}")
        return extract_metadata
    
    ticker = extract_metadata['ticker']
    logger.info(f"Starting bootstrap transformation for ticker {ticker}: {extract_metadata['rows']} rows")
    
    try:
        # Load the extracted data from file
        extract_file_path = extract_metadata.get('file_path')
        if not extract_file_path or not os.path.exists(extract_file_path):
            raise ValueError(f"Bootstrap extracted data file not found: {extract_file_path}")
        
        # Read DataFrame from Parquet file
        df = pd.read_parquet(extract_file_path)
        logger.info(f'Loaded bootstrap extracted data for {ticker} from {extract_file_path} ({os.path.getsize(extract_file_path)} bytes)')
        
        # Debug: Log DataFrame structure
        logger.info(f'DataFrame shape for {ticker}: {df.shape}')
        logger.info(f'DataFrame columns for {ticker}: {df.columns.tolist()}')
        
        # Get transformation parameters from Variables
        ema_spans = Variable.get('EMA_SPANS', [9, 20, 50], deserialize_json=True)
        volatility_window = Variable.get('VOLATILITY_WINDOW', 21, deserialize_json=False)
        
        logger.info(f'Applying transformations to {ticker}: EMA spans {ema_spans}, volatility window {volatility_window}')
        
        # Transform the data
        metrics = calculate_financial_metrics(
            df, 
            ema_spans=ema_spans, 
            volatility_window=int(volatility_window)
        )
        
        # Store the transformed data as Parquet file in shared volume
        file_path = f"/tmp/pipeline_data/bootstrap_transform_{context['run_id']}_{ticker}.parquet"
        metrics.to_parquet(file_path)
        logger.info(f'Saved bootstrap transformed data for {ticker} to {file_path} ({os.path.getsize(file_path)} bytes)')
        
        # Clean up the input file
        if os.path.exists(extract_file_path):
            os.unlink(extract_file_path)
            logger.info(f'Cleaned up input file: {extract_file_path}')
        
        # Return metadata for downstream tasks
        transform_metadata = {
            'status': 'success',
            'ticker': ticker,
            'file_path': file_path,
            'rows': len(metrics),
            'columns': list(metrics.columns),
            'date_range': extract_metadata['date_range'],
            'execution_date': extract_metadata['execution_date'],
            'transformation_params': {
                'ema_spans': ema_spans,
                'volatility_window': volatility_window
            }
        }
        
        logger.info(f'Bootstrap transformation completed for {ticker}: {len(metrics)} rows processed')
        return transform_metadata
        
    except Exception as e:
        logger.error(f'Bootstrap transformation failed for {ticker}: {e}')
        raise


def bootstrap_load_single_ticker(transform_metadata: dict, **context) -> dict:
    '''Load bootstrap transformed data for a single ticker into PostgreSQL.'''
    logger = logging.getLogger(__name__)
    
    if transform_metadata.get('status') == 'failed':
        logger.info(f"Skipping load for {transform_metadata.get('ticker')}: {transform_metadata.get('reason')}")
        return transform_metadata
    
    ticker = transform_metadata['ticker']
    logger.info(f"Starting bootstrap load for ticker {ticker}: {transform_metadata['rows']} rows")
    
    try:
        # Load the transformed data from file
        transform_file_path = transform_metadata.get('file_path')
        if not transform_file_path or not os.path.exists(transform_file_path):
            raise ValueError(f"Bootstrap transformed data file not found: {transform_file_path}")
        
        # Read DataFrame from Parquet file
        metrics = pd.read_parquet(transform_file_path)
        logger.info(f'Loaded bootstrap transformed data for {ticker} from {transform_file_path} ({os.path.getsize(transform_file_path)} bytes)')
        
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
        
        logger.info(f'Loading bootstrap data for {ticker} into table: {table_name}')
        
        # Load to database
        load_to_postgres(metrics, table_name=table_name, db_config=db_config)
        
        # Clean up the transformed data file
        if os.path.exists(transform_file_path):
            os.unlink(transform_file_path)
            logger.info(f'Cleaned up transformed file: {transform_file_path}')
        
        # Return load metadata
        load_metadata = {
            'status': 'success',
            'ticker': ticker,
            'table_name': table_name,
            'rows_loaded': len(metrics),
            'date_range': transform_metadata['date_range'],
            'execution_date': transform_metadata['execution_date'],
            'load_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f'Bootstrap load completed for {ticker}: {len(metrics)} rows loaded into {table_name}')
        return load_metadata
        
    except Exception as e:
        logger.error(f'Bootstrap load failed for {ticker}: {e}')
        raise


def bootstrap_aggregate_results(load_results: List[dict], **context) -> dict:
    '''Aggregate results from all ticker processing tasks.'''
    logger = logging.getLogger(__name__)
    
    logger.info(f'Aggregating results from {len(load_results)} ticker tasks')
    
    try:
        successful_tickers = []
        failed_tickers = []
        total_rows_loaded = 0
        
        for result in load_results:
            ticker = result.get('ticker', 'unknown')
            if result.get('status') == 'success':
                successful_tickers.append(ticker)
                total_rows_loaded += result.get('rows_loaded', 0)
            else:
                failed_tickers.append({
                    'ticker': ticker,
                    'reason': result.get('reason', 'unknown_error')
                })
        
        # Aggregate metadata
        aggregate_result = {
            'status': 'success' if not failed_tickers else 'partial_success' if successful_tickers else 'failed',
            'execution_date': context['execution_date'].isoformat(),
            'aggregation_timestamp': datetime.now().isoformat(),
            'total_tickers_processed': len(load_results),
            'successful_tickers': successful_tickers,
            'failed_tickers': failed_tickers,
            'total_rows_loaded': total_rows_loaded,
            'success_rate': len(successful_tickers) / len(load_results) if load_results else 0
        }
        
        logger.info(f'Aggregation completed: {len(successful_tickers)} successful, {len(failed_tickers)} failed, {total_rows_loaded} total rows')
        return aggregate_result
        
    except Exception as e:
        logger.error(f'Bootstrap aggregation failed: {e}')
        raise


def bootstrap_validation(aggregate_result: dict, **context) -> dict:
    '''Validate that the bootstrap pipeline executed successfully.
    
    Performs both pipeline-level validation and data quality validation:
    - Pipeline execution status across all tickers
    - Row count validation (> 0 for each ticker)
    - Date continuity validation (no gaps in date sequences)
    - Ticker whitelist validation (all processed tickers are allowed)
    '''
    logger = logging.getLogger(__name__)
    
    logger.info('Validating bootstrap pipeline execution')
    
    try:
        # Get configuration for data quality validation
        table_name = Variable.get('TABLE_NAME', 'price_metrics', deserialize_json=False)
        lookback_days = Variable.get('LOOKBACK_DAYS', 30, deserialize_json=False)
        db_config = get_db_config_with_fallback()
        
        # Determine overall success
        overall_status = aggregate_result.get('status', 'unknown')
        successful_tickers = aggregate_result.get('successful_tickers', [])
        failed_tickers = aggregate_result.get('failed_tickers', [])
        
        # Pipeline-level validation result
        validation_result = {
            'status': overall_status,
            'execution_date': context['execution_date'].isoformat(),
            'validation_timestamp': datetime.now().isoformat(),
            'total_tickers_processed': aggregate_result.get('total_tickers_processed', 0),
            'successful_tickers': successful_tickers,
            'failed_tickers': failed_tickers,
            'total_rows_loaded': aggregate_result.get('total_rows_loaded', 0),
            'success_rate': aggregate_result.get('success_rate', 0)
        }
        
        if overall_status == 'failed':
            logger.error(f'Bootstrap pipeline failed: all {len(failed_tickers)} tickers failed')
        elif overall_status == 'partial_success':
            logger.warning(f'Bootstrap pipeline partially successful: {len(successful_tickers)} succeeded, {len(failed_tickers)} failed')
        else:
            logger.info(f'Bootstrap pipeline completed successfully: all {len(successful_tickers)} tickers processed')
        
        # Perform data quality validation if we have successful tickers
        if successful_tickers:
            logger.info('Performing data quality validation for bootstrap')
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
        logger.error(f'Bootstrap validation failed: {e}')
        raise


# Define the bootstrap DAG with dynamic task generation
def create_bootstrap_dag():
    '''Create bootstrap DAG with dynamic task generation for each ticker.'''
    
    dag = DAG(
        dag_id='bootstrap_stock_data_dag',
        description='One-time bootstrap DAG to fetch historical stock data with batching per ticker',
        default_args=BOOTSTRAP_ARGS,
        schedule_interval=None,  # Manual trigger only
        start_date=datetime(2025, 8, 23),  # Recent date
        catchup=False,  # No catchup for bootstrap
        tags=['stocks', 'etl', 'bootstrap', 'one-time', 'batched'],
        max_active_runs=1,  # Only one bootstrap run at a time
        doc_md='''
        ## Bootstrap Stock Data DAG (Batched)
        
        This DAG is designed for one-time historical data bootstrap with batching per ticker:
        
        - **Manual Trigger Only**: Must be triggered manually via Airflow UI or CLI
        - **Historical Data**: Fetches data from 2019-01-01 to yesterday
        - **Batched Processing**: Creates separate tasks for each ticker for parallel processing
        - **Rate Limiting Protection**: Built-in delays to prevent API overwhelming
        - **Error Isolation**: Failed tickers don't affect successful ones
        - **One-time Use**: After bootstrap, use the main stock_pipeline_dag for daily updates
        
        ### Usage:
        1. **Fixed Date Range**: This DAG automatically fetches data from 2019-01-01 to yesterday
        2. **Simple Trigger**: Just trigger the DAG manually - no configuration needed
        3. After completion, the main pipeline will handle daily updates
        4. This DAG can be paused or deleted after successful bootstrap
        
        ### Configuration Variables:
        - Uses the same variables as the main pipeline
        - 'TICKERS': Comma-separated list of stock tickers
        - 'INTERVAL': Data interval (1d, 1wk, 1mo)
        - 'TABLE_NAME': Target PostgreSQL table
        - 'EMA_SPANS': List of EMA periods for technical indicators
        - 'VOLATILITY_WINDOW': Rolling window for volatility calculation
        
        ### Architecture:
        - Dynamic task generation creates ETL pipeline for each ticker
        - Parallel processing with error isolation
        - File-based data passing to avoid XCom size limits
        - Aggregation and validation of all ticker results
        ''',
    )
    
    # Get tickers from Airflow Variables
    tickers_str = Variable.get('TICKERS', 'NVDA,AAPL,MSFT,GOOGL', deserialize_json=False)
    if isinstance(tickers_str, str):
        tickers = [t.strip() for t in tickers_str.split(',') if t.strip()]
    else:
        tickers = tickers_str
    
    # Create tasks for each ticker
    load_tasks = []
    
    for ticker in tickers:
        # Create wrapper functions for this ticker
        def create_transform_wrapper(ticker_name):
            def transform_wrapper(**context):
                task_instance = context['task_instance']
                extract_metadata = task_instance.xcom_pull(task_ids=f'extract_{ticker_name}')
                return bootstrap_transform_single_ticker(extract_metadata, **context)
            return transform_wrapper
        
        def create_load_wrapper(ticker_name):
            def load_wrapper(**context):
                task_instance = context['task_instance']
                transform_metadata = task_instance.xcom_pull(task_ids=f'transform_{ticker_name}')
                return bootstrap_load_single_ticker(transform_metadata, **context)
            return load_wrapper
        
        # Extract task for this ticker
        extract_task = PythonOperator(
            task_id=f'extract_{ticker}',
            python_callable=bootstrap_extract_single_ticker,
            op_args=[ticker],
            dag=dag,
        )
        
        # Transform task for this ticker
        transform_task = PythonOperator(
            task_id=f'transform_{ticker}',
            python_callable=create_transform_wrapper(ticker),
            dag=dag,
        )
        
        # Load task for this ticker
        load_task = PythonOperator(
            task_id=f'load_{ticker}',
            python_callable=create_load_wrapper(ticker),
            dag=dag,
        )
        
        # Set up dependencies for this ticker's pipeline
        extract_task >> transform_task >> load_task
        
        load_tasks.append(load_task)
    
    # Create a function to collect load results
    def collect_load_results(**context):
        task_instance = context['task_instance']
        load_results = []
        for ticker in tickers:
            result = task_instance.xcom_pull(task_ids=f'load_{ticker}')
            if result:
                load_results.append(result)
        return bootstrap_aggregate_results(load_results, **context)
    
    def collect_and_validate(**context):
        task_instance = context['task_instance']
        aggregate_result = task_instance.xcom_pull(task_ids='aggregate_results')
        return bootstrap_validation(aggregate_result, **context)
    
    # Aggregation task that waits for all load tasks
    aggregate_task = PythonOperator(
        task_id='aggregate_results',
        python_callable=collect_load_results,
        dag=dag,
    )
    
    # Validation task
    validation_task = PythonOperator(
        task_id='validation',
        python_callable=collect_and_validate,
        dag=dag,
    )
    
    # Set up final dependencies
    load_tasks >> aggregate_task >> validation_task
    
    return dag


# Create the bootstrap DAG instance
bootstrap_dag = create_bootstrap_dag()

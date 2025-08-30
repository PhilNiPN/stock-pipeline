'''
This script is used to run the pipeline manually, for development purposes.
'''

from __future__ import annotations

import os
from datetime import date
from typing import List

import pandas as pd
from dotenv import load_dotenv

from src.extract import fetch_stock_data
from src.transform import calculate_financial_metrics
from src.load import load_to_postgres
from src.utils import get_db_config
from src.utils import get_next_start_dates  # make sure this exists per snippet below

load_dotenv()

def _parse_tickers(value: str | None) -> List[str]:
    if not value:
        return ['NVDA']
    return [t.strip() for t in value.split(',') if t.strip()]


def main() -> None:
    load_dotenv()  # read .env

    # Read config from environment (with sensible defaults). When no
    # TABLE_NAME is supplied we default to the same table used by our
    # Airflow DAG and SQL DDL (price_metrics) rather than the
    # previously hard‑coded stock_metrics. This prevents creating
    # mismatched tables when running the CLI manually.
    tickers = _parse_tickers(os.getenv('TICKERS', 'NVDA'))
    interval = os.getenv('INTERVAL', '1d')
    table_name = os.getenv('TABLE_NAME', 'price_metrics')

    # Optional END_DATE override; otherwise use today
    end_date = os.getenv('END_DATE')
    if not end_date:
        end_date = date.today().strftime('%Y-%m-%d')

    db_config = get_db_config()

    # Ask DB what the next start date should be for each ticker (max(date)+1)
    next_starts = get_next_start_dates(db_config, table_name, tickers)

    batches: list[pd.DataFrame] = []

    for t in tickers:
        start = next_starts.get(t)
        if not start:
            # Bootstrap if no history for this ticker yet
            start = os.getenv('START_DATE', '2015-01-01')

        # If start is already beyond end_date, skip
        if pd.to_datetime(start) > pd.to_datetime(end_date):
            print(f'[skip] {t}: nothing new to fetch (start {start} > end {end_date})')
            continue

        print(f'[fetch] {t}: {start} → {end_date} ({interval})')
        df = fetch_stock_data(t, start_date=start, end_date=end_date, interval=interval)
        if df is None or df.empty:
            print(f'[info] {t}: no rows returned')
            continue

        metrics = calculate_financial_metrics(df)
        # Ensure ticker/date columns exist and are clean if your transform doesn't add ticker
        if 'ticker' not in metrics.columns:
            metrics['ticker'] = t
        metrics['date'] = pd.to_datetime(metrics['date']).dt.date

        batches.append(metrics)

    if not batches:
        print('[done] No new data to load.')
        return

    all_metrics = pd.concat(batches, ignore_index=True)

    # Load with idempotent UPSERT (src/load.py handles table creation + PK)
    load_to_postgres(
        all_metrics,
        table_name=table_name,
        db_config=db_config,
    )
    print(f'[done] Upserted {len(all_metrics)} rows into {table_name}.')


if __name__ == '__main__':
    main()



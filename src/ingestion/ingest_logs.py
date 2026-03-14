"""ETL pipeline: parse real CSV logs → aggregate into per-minute metrics → SQLite.

This replaces the OpenObserve dependency entirely for the offline sandbox model.
Parsed logs are aggregated into time-bucketed error_rate and latency_p95 metrics
and written directly into the existing `metrics` table (source='logs').
This lets the existing baseline + detection pipeline consume them unchanged.
"""
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager
from src.utils.log_parser import LogParser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Bucket size for time-series aggregation
BUCKET_MINUTES = 5


def aggregate_to_metrics(df: pd.DataFrame, service_name: str) -> List[Dict]:
    """Aggregate parsed log DataFrame into per-bucket metric rows.

    Produces two metric series per service:
      - error_rate  : errors per minute in the bucket
      - latency_p95 : 95th-percentile latency_ms in the bucket

    Args:
        df: Parsed DataFrame from LogParser.parse_csv_file()
        service_name: service name string

    Returns:
        List of dicts ready for db.insert_metric()
    """
    if df.empty:
        return []

    # Ensure we have a proper datetime index
    df = df.copy()
    df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'], utc=True, errors='coerce')
    df = df.dropna(subset=['timestamp_dt'])
    if df.empty:
        return []

    # Remove tz for grouping (SQLite stores naive ISO strings)
    df['ts_naive'] = df['timestamp_dt'].dt.tz_localize(None)
    df = df.set_index('ts_naive').sort_index()

    metric_rows = []
    freq = f'{BUCKET_MINUTES}min'

    # --- error_rate ---
    err_series = df['is_error'].resample(freq).agg(['sum', 'count'])
    err_series['error_rate'] = (err_series['sum'] / err_series['count'].clip(lower=1)) * 100
    for ts, row in err_series.iterrows():
        if row['count'] == 0:
            continue
        metric_rows.append({
            'timestamp':    ts.isoformat(),
            'service_name': service_name,
            'metric_name':  'error_rate',
            'metric_value': round(float(row['error_rate']), 4),
            'metric_type':  'error_rate',
            'source':       'logs',
        })

    # --- latency_p95 (only for rows that have latency data) ---
    lat_df = df.dropna(subset=['latency_ms'])
    if not lat_df.empty:
        lat_series = lat_df['latency_ms'].resample(freq).quantile(0.95)
        for ts, val in lat_series.items():
            if pd.isna(val):
                continue
            metric_rows.append({
                'timestamp':    ts.isoformat(),
                'service_name': service_name,
                'metric_name':  'latency_p95',
                'metric_value': round(float(val), 2),
                'metric_type':  'latency',
                'source':       'logs',
            })

    # --- 5xx_rate ---
    http_df = df.dropna(subset=['status_code']).copy()
    if not http_df.empty:
        http_df['is_5xx'] = http_df['status_code'].astype(float) >= 500
        s5xx = http_df['is_5xx'].resample(freq).agg(['sum', 'count'])
        s5xx['rate'] = (s5xx['sum'] / s5xx['count'].clip(lower=1)) * 100
        for ts, row in s5xx.iterrows():
            if row['count'] == 0:
                continue
            metric_rows.append({
                'timestamp':    ts.isoformat(),
                'service_name': service_name,
                'metric_name':  'http_5xx_rate',
                'metric_value': round(float(row['rate']), 4),
                'metric_type':  'error_rate',
                'source':       'logs',
            })

    return metric_rows


def ingest_csv_file(filepath: Path, parser: LogParser, db: DatabaseManager) -> int:
    """Parse one CSV, aggregate, and write metrics to SQLite.

    Returns:
        Number of metric rows inserted.
    """
    service_name = filepath.stem
    logger.info(f"Ingesting {filepath.name}  (service={service_name})")

    df = parser.parse_csv_file(str(filepath), service_name)
    if df.empty:
        logger.warning(f"  No logs parsed from {filepath.name}")
        return 0

    logger.info(
        f"  Parsed {len(df)} log lines  "
        f"| errors={df['is_error'].sum()}  "
        f"| latency rows={df['latency_ms'].notna().sum()}"
    )

    metrics = aggregate_to_metrics(df, service_name)
    if not metrics:
        logger.warning(f"  No metrics aggregated for {service_name}")
        return 0

    inserted = 0
    for m in metrics:
        try:
            db.insert_metric(
                timestamp=m['timestamp'],
                service_name=m['service_name'],
                metric_name=m['metric_name'],
                metric_value=m['metric_value'],
                metric_type=m['metric_type'],
                source=m['source'],
            )
            inserted += 1
        except Exception:
            pass  # UNIQUE constraint on duplicate bucket — safe to skip

    logger.info(f"  Wrote {inserted} metric rows for {service_name}")
    return inserted


def main():
    """Ingest all CSV log files from data/raw_logs/ into SQLite."""
    logger.info("Starting log ingestion pipeline")

    raw_logs_dir = Path("data/raw_logs")
    if not raw_logs_dir.exists():
        logger.error(f"Directory not found: {raw_logs_dir}")
        logger.info("Creating directory — please copy CSV files there and re-run.")
        raw_logs_dir.mkdir(parents=True, exist_ok=True)
        return 1

    csv_files = sorted(raw_logs_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files in {raw_logs_dir}")
        return 1

    parser = LogParser()
    db = DatabaseManager()
    total_rows = 0

    for csv_file in csv_files:
        total_rows += ingest_csv_file(csv_file, parser, db)

    stats = parser.get_stats()
    logger.info("\n=== INGESTION SUMMARY ===")
    logger.info(f"Files processed : {len(csv_files)}")
    logger.info(f"Total log lines : {stats['total_lines']}")
    logger.info(f"Parsed          : {stats['parsed_lines']}")
    logger.info(f"Failed          : {stats['failed_lines']}")
    logger.info(f"Metric rows     : {total_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

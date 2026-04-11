"""ETL pipeline: parse real CSV logs → aggregate into per-minute metrics → SQLite.

Supports two sources via --source flag:
  csv         (default) — read CSV files from data/raw_logs/
  opensearch            — pull live data from OpenSearch/OpenObserve
  both                  — run CSV first, then OpenSearch

Parsed logs are aggregated into time-bucketed error_rate and latency_p95 metrics
and written directly into the existing `metrics` table (source='logs' or
source='opensearch').
"""
import sys
import argparse
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

# Bucket size for time-series aggregation — keep in sync with
# opensearch_ingest._BUCKET_MINUTES so metrics are comparable
BUCKET_MINUTES = 5


def aggregate_to_metrics(df: pd.DataFrame, service_name: str) -> List[Dict]:
    """Aggregate parsed log DataFrame into per-bucket metric rows.

    Produces up to three metric series per service:
      - error_rate   : errors per minute in the bucket (% of total)
      - latency_p95  : 95th-percentile latency_ms in the bucket
      - http_5xx_rate: HTTP 5xx responses as % of total in the bucket

    Args:
        df: Parsed DataFrame — must contain columns:
              timestamp_dt (datetime), is_error (0/1),
              latency_ms (float, nullable), status_code (float, nullable)
        service_name: service name string

    Returns:
        List of dicts ready for db.insert_metric()
    """
    if df.empty:
        return []

    df = df.copy()
    df['timestamp_dt'] = pd.to_datetime(df['timestamp_dt'], utc=True, errors='coerce')
    df = df.dropna(subset=['timestamp_dt'])
    if df.empty:
        return []

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

    # --- latency_p95 ---
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

    # --- http_5xx_rate ---
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

    Returns number of metric rows inserted.
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


def run_csv(db: DatabaseManager) -> int:
    """Ingest all CSV log files from data/raw_logs/ into SQLite."""
    logger.info("[CSV] Starting log ingestion pipeline")

    raw_logs_dir = Path("data/raw_logs")
    if not raw_logs_dir.exists():
        logger.error(f"Directory not found: {raw_logs_dir}")
        logger.info("Creating directory — copy CSV files there and re-run.")
        raw_logs_dir.mkdir(parents=True, exist_ok=True)
        return 0

    csv_files = sorted(raw_logs_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files in {raw_logs_dir}")
        return 0

    parser = LogParser()
    total_rows = 0

    for csv_file in csv_files:
        total_rows += ingest_csv_file(csv_file, parser, db)

    stats = parser.get_stats()
    logger.info("\n=== CSV INGESTION SUMMARY ===")
    logger.info(f"Files processed : {len(csv_files)}")
    logger.info(f"Total log lines : {stats['total_lines']}")
    logger.info(f"Parsed          : {stats['parsed_lines']}")
    logger.info(f"Failed          : {stats['failed_lines']}")
    logger.info(f"Metric rows     : {total_rows}")
    return total_rows


def run_opensearch(hours: int = 24, use_scroll: bool = False,
                   service: Optional[str] = None) -> int:
    """Delegate to opensearch_ingest.OpenSearchIngestor."""
    from src.ingestion.opensearch_ingest import OpenSearchIngestor
    ingestor = OpenSearchIngestor(
        hours=hours,
        use_scroll=use_scroll,
        target_service=service,
    )
    return ingestor.run()


def main():
    parser = argparse.ArgumentParser(
        description="Ingest logs into SQLite from CSV files or OpenSearch"
    )
    parser.add_argument(
        "--source",
        choices=["csv", "opensearch", "both"],
        default="csv",
        help="Data source: csv (default) | opensearch | both",
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Hours to look back when using OpenSearch source (default: 24)"
    )
    parser.add_argument(
        "--scroll", action="store_true",
        help="Use scroll API instead of aggregations (OpenSearch source only)"
    )
    parser.add_argument(
        "--service", type=str, default=None,
        help="Limit OpenSearch ingestion to a single service"
    )
    args = parser.parse_args()

    db    = DatabaseManager()
    total = 0

    if args.source in ("csv", "both"):
        total += run_csv(db)

    if args.source in ("opensearch", "both"):
        total += run_opensearch(
            hours=args.hours,
            use_scroll=args.scroll,
            service=args.service,
        )

    logger.info(f"\nTotal metric rows written: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

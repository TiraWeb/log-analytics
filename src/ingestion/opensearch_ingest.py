"""Ingest metrics from OpenSearch / OpenObserve into SQLite.

Pipeline
--------
1. Discover services via terms aggregation (or read from config).
2. For each service, iterate over time-windowed sub-windows
   (default: 1-hour windows across the requested look-back period).
3. Inside each sub-window, fire a single date_histogram aggregation
   (5-minute buckets) — this is pure server-side computation, so it
   handles the 6.5 TB/month volume without pulling raw docs.
4. Convert each bucket → metric rows (error_rate, latency_p95,
   http_5xx_rate, log_volume) and write to SQLite.
5. Optionally fall back to raw scroll for services that have non-standard
   fields (pass --scroll flag).

Usage
-----
    python src/ingestion/opensearch_ingest.py
    python src/ingestion/opensearch_ingest.py --hours 48
    python src/ingestion/opensearch_ingest.py --hours 6 --scroll
    python src/ingestion/opensearch_ingest.py --hours 24 --service my-service
"""
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.opensearch_client import OpenSearchClient
from src.utils.db_utils import DatabaseManager
from src.utils.log_parser import LogParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# How large a sub-window to send to OpenSearch in one agg request.
# Smaller = less memory pressure on the cluster; larger = fewer round-trips.
_SUB_WINDOW_HOURS = 1
# Bucket granularity passed to date_histogram_agg (must match BUCKET_MINUTES
# in ingest_logs.py so metrics from both sources are comparable).
_BUCKET_MINUTES = 5


class OpenSearchIngestor:
    """Pull aggregated metrics from OpenSearch and write to SQLite."""

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        hours: int = 24,
        use_scroll: bool = False,
        target_service: Optional[str] = None,
    ):
        self.client  = OpenSearchClient(config_path)
        self.db      = DatabaseManager(config_path)
        self.parser  = LogParser()
        self.hours   = hours
        self.use_scroll = use_scroll
        self.target_service = target_service

    # ── Time helpers ─────────────────────────────────────────────────────────

    def _windows(self, start: datetime, end: datetime):
        """Yield (win_start, win_end) sub-windows of _SUB_WINDOW_HOURS."""
        cur = start
        delta = timedelta(hours=_SUB_WINDOW_HOURS)
        while cur < end:
            win_end = min(cur + delta, end)
            yield cur, win_end
            cur = win_end

    # ── Aggregation-based ingestion (primary path) ────────────────────────────

    def _buckets_to_metrics(self, buckets: List[Dict], service_name: str) -> List[Dict]:
        """Convert date_histogram buckets → metric row dicts."""
        rows = []
        for b in buckets:
            # Epoch-ms → naive ISO string (SQLite stores naive timestamps)
            ts = datetime.utcfromtimestamp(b["timestamp_ms"] / 1000).isoformat()
            doc_count = b["doc_count"]
            if doc_count == 0:
                continue

            # error_rate — % of logs that are ERROR/FATAL/CRITICAL
            error_rate = round((b["error_count"] / doc_count) * 100, 4)
            rows.append({
                "timestamp":    ts,
                "service_name": service_name,
                "metric_name":  "error_rate",
                "metric_value": error_rate,
                "metric_type":  "error_rate",
                "source":       "opensearch",
            })

            # latency_p95
            if b["p95_latency"] and b["p95_latency"] > 0:
                rows.append({
                    "timestamp":    ts,
                    "service_name": service_name,
                    "metric_name":  "latency_p95",
                    "metric_value": round(b["p95_latency"], 2),
                    "metric_type":  "latency",
                    "source":       "opensearch",
                })

            # http_5xx_rate
            http_5xx_rate = round((b["count_5xx"] / doc_count) * 100, 4)
            rows.append({
                "timestamp":    ts,
                "service_name": service_name,
                "metric_name":  "http_5xx_rate",
                "metric_value": http_5xx_rate,
                "metric_type":  "error_rate",
                "source":       "opensearch",
            })

            # log_volume — raw doc count per bucket (useful for volume anomalies)
            rows.append({
                "timestamp":    ts,
                "service_name": service_name,
                "metric_name":  "log_volume",
                "metric_value": float(doc_count),
                "metric_type":  "count",
                "source":       "opensearch",
            })

        return rows

    def ingest_service_agg(self, service_name: str, start: datetime, end: datetime) -> int:
        """Ingest one service using aggregations. Returns rows written."""
        inserted = 0
        for win_start, win_end in self._windows(start, end):
            buckets = self.client.date_histogram_agg(
                service_name=service_name,
                start=win_start,
                end=win_end,
                interval_minutes=_BUCKET_MINUTES,
            )
            if not buckets:
                continue

            metric_rows = self._buckets_to_metrics(buckets, service_name)
            for m in metric_rows:
                try:
                    self.db.insert_metric(
                        timestamp=m["timestamp"],
                        service_name=m["service_name"],
                        metric_name=m["metric_name"],
                        metric_value=m["metric_value"],
                        metric_type=m["metric_type"],
                        source=m["source"],
                    )
                    inserted += 1
                except Exception:
                    pass  # UNIQUE conflict on duplicate bucket — safe

            logger.debug(
                f"  {service_name} | {win_start.strftime('%H:%M')}–{win_end.strftime('%H:%M')}"
                f" | {len(buckets)} buckets → {len(metric_rows)} metric rows"
            )

        return inserted

    # ── Scroll-based ingestion (fallback / raw parsing) ───────────────────────

    def ingest_service_scroll(self, service_name: str, start: datetime, end: datetime) -> int:
        """Ingest one service by scrolling raw docs → LogParser → aggregate.

        Only use this when the index uses non-standard field names that
        the aggregation path can't handle, or when you need raw log text.
        """
        import pandas as pd
        from src.ingestion.ingest_logs import aggregate_to_metrics

        inserted = 0
        raw_docs: List[Dict] = []

        fields = [
            self.client.field_timestamp,
            self.client.field_service,
            self.client.field_level,
            self.client.field_latency,
            self.client.field_status_code,
            "message",
        ]

        logger.info(f"  Scrolling raw docs for {service_name} …")
        for doc in self.client.scroll_docs(
            service_name=service_name,
            start=start,
            end=end,
            fields=fields,
        ):
            raw_docs.append(doc)
            # Flush every 50k docs to control memory
            if len(raw_docs) >= 50_000:
                inserted += self._flush_scroll_batch(raw_docs, service_name)
                raw_docs = []

        if raw_docs:
            inserted += self._flush_scroll_batch(raw_docs, service_name)

        return inserted

    def _flush_scroll_batch(self, raw_docs: List[Dict], service_name: str) -> int:
        """Parse a batch of raw docs and write aggregated metrics."""
        import pandas as pd
        from src.ingestion.ingest_logs import aggregate_to_metrics

        rows = []
        ts_field  = self.client.field_timestamp
        lvl_field = self.client.field_level
        lat_field = self.client.field_latency
        sc_field  = self.client.field_status_code

        for doc in raw_docs:
            raw_ts = doc.get(ts_field)
            # OpenSearch timestamps can be epoch-ms int or ISO string
            if isinstance(raw_ts, (int, float)):
                ts = datetime.utcfromtimestamp(raw_ts / 1000)
            else:
                try:
                    ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                    ts = ts.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

            level = str(doc.get(lvl_field, "")).upper()
            is_error = level in ("ERROR", "FATAL", "CRITICAL")

            latency = doc.get(lat_field)
            try:
                latency = float(latency) if latency is not None else None
            except (ValueError, TypeError):
                latency = None

            sc = doc.get(sc_field)
            try:
                sc = int(sc) if sc is not None else None
            except (ValueError, TypeError):
                sc = None

            rows.append({
                "timestamp_dt": ts,
                "is_error":     int(is_error),
                "latency_ms":   latency,
                "status_code":  sc,
                "service_name": service_name,
            })

        if not rows:
            return 0

        df = pd.DataFrame(rows)
        metrics = aggregate_to_metrics(df, service_name)
        # Override source so we can distinguish scroll vs agg in the DB
        for m in metrics:
            m["source"] = "opensearch"

        inserted = 0
        for m in metrics:
            try:
                self.db.insert_metric(
                    timestamp=m["timestamp"],
                    service_name=m["service_name"],
                    metric_name=m["metric_name"],
                    metric_value=m["metric_value"],
                    metric_type=m["metric_type"],
                    source=m["source"],
                )
                inserted += 1
            except Exception:
                pass
        return inserted

    # ── Main run ──────────────────────────────────────────────────────────────

    def run(self) -> int:
        """Run full ingestion. Returns total metric rows written."""
        if not self.client.check_health():
            logger.error(
                "OpenSearch is not reachable — check config/config.yaml "
                "[opensearch] section and ensure the cluster is running."
            )
            return 0

        end   = datetime.now(timezone.utc)
        start = end - timedelta(hours=self.hours)
        logger.info(
            f"OpenSearch ingest — last {self.hours}h  "
            f"({start.strftime('%Y-%m-%d %H:%M')} → {end.strftime('%Y-%m-%d %H:%M')} UTC)"
        )

        # Service list: use --service flag, else auto-discover
        if self.target_service:
            services = [self.target_service]
        else:
            services = self.client.discover_services(start, end)
            if not services:
                logger.warning(
                    "No services discovered — check that index has data in the "
                    "requested time window and that field_service is correct."
                )
                return 0

        logger.info(f"Processing {len(services)} service(s): {services}")

        total = 0
        for svc in services:
            logger.info(f"Ingesting: {svc}")
            if self.use_scroll:
                n = self.ingest_service_scroll(svc, start, end)
            else:
                n = self.ingest_service_agg(svc, start, end)
            logger.info(f"  → {n} metric rows written")
            total += n

        # Print index stats so we can see scale
        stats = self.client.get_index_stats()
        if stats:
            size_gb = stats["size_bytes"] / (1024 ** 3)
            logger.info(
                f"\nIndex stats: {stats['doc_count']:,} docs, "
                f"{size_gb:.2f} GB on-disk"
            )

        logger.info(f"\n=== OS INGEST SUMMARY ===")
        logger.info(f"Services processed : {len(services)}")
        logger.info(f"Total metric rows  : {total}")
        return total


def main():
    parser = argparse.ArgumentParser(
        description="Ingest metrics from OpenSearch/OpenObserve into SQLite"
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="How many hours back to pull (default: 24)"
    )
    parser.add_argument(
        "--scroll", action="store_true",
        help="Use scroll API instead of aggregations (slower, more detailed)"
    )
    parser.add_argument(
        "--service", type=str, default=None,
        help="Limit ingestion to a single service name"
    )
    args = parser.parse_args()

    ingestor = OpenSearchIngestor(
        hours=args.hours,
        use_scroll=args.scroll,
        target_service=args.service,
    )
    return 0 if ingestor.run() >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())

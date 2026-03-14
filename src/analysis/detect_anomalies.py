"""Detect anomalies using Z-score statistical profiling.

Supports two signal sources:
  1. 'logs'      — error_rate / latency_p95 / http_5xx_rate derived from real log CSVs
  2. 'synthetic' — infra metrics (cpu_usage, memory_usage, asg_capacity, db_connections)

Log-derived metrics are the PRIMARY trigger for incident creation.
Infra metrics are used later by diagnose_root_cause.py for cross-correlation.
"""
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Metric names that come from real logs — these are the primary incident trigger
LOG_METRICS   = {'error_rate', 'latency_p95', 'http_5xx_rate'}
# Infra metrics — secondary, used for correlation only
INFRA_METRICS = {'cpu_usage', 'memory_usage', 'asg_capacity', 'db_connections'}


class AnomalyDetector:
    """Z-score anomaly detector that respects the two-signal model."""

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.db = DatabaseManager(config_path)
        self.threshold_sigma = config['analysis']['anomaly_threshold_sigma']

    def get_baseline(self, service_name: str, metric_name: str) -> Optional[Dict]:
        rows = self.db.execute_query(
            """SELECT * FROM baselines
               WHERE service_name = ? AND metric_name = ?
               ORDER BY created_at DESC LIMIT 1""",
            (service_name, metric_name)
        )
        return rows[0] if rows else None

    def calculate_zscore(self, value: float, baseline: Dict) -> float:
        if baseline['stddev'] == 0:
            return 0.0
        return (value - baseline['mean']) / baseline['stddev']

    def detect_for_service(self, service_name: str, source_filter: Optional[str] = None) -> List[Dict]:
        """Return all anomalous readings for a service.

        Args:
            service_name: service to scan
            source_filter: if given, only scan metrics with this source value
        """
        query = """
            SELECT timestamp, metric_name, metric_value, source
            FROM   metrics
            WHERE  service_name = ?
        """
        params = [service_name]
        if source_filter:
            query += " AND source = ?"
            params.append(source_filter)
        query += " ORDER BY timestamp DESC LIMIT 5000"

        results = self.db.execute_query(query, tuple(params))
        if not results:
            return []

        df = pd.DataFrame(results)
        anomalies = []

        for metric_name in df['metric_name'].unique():
            baseline = self.get_baseline(service_name, metric_name)
            if not baseline:
                continue

            for _, row in df[df['metric_name'] == metric_name].iterrows():
                z = self.calculate_zscore(row['metric_value'], baseline)
                if abs(z) >= self.threshold_sigma:
                    anomalies.append({
                        'timestamp':      row['timestamp'],
                        'service_name':   service_name,
                        'metric_name':    metric_name,
                        'value':          row['metric_value'],
                        'baseline_mean':  baseline['mean'],
                        'zscore':         z,
                        'severity':       'high' if abs(z) >= 4 else 'medium',
                        'source':         row['source'],
                    })
                    logger.info(
                        f"  \u26a0  {metric_name}: {row['metric_value']:.2f} "
                        f"(baseline: {baseline['mean']:.2f}, z={z:.2f}, src={row['source']})"
                    )
        return anomalies

    def classify_incident_type(self, anomalies: List[Dict]) -> str:
        """Classify incident based on which metrics are anomalous."""
        names = {a['metric_name'] for a in anomalies}

        # Log-signal classifications (primary)
        if 'error_rate' in names and 'http_5xx_rate' in names:
            return 'error_spike'
        if 'error_rate' in names:
            return 'error_spike'
        if 'http_5xx_rate' in names:
            return 'error_spike'
        if 'latency_p95' in names:
            return 'latency_spike'

        # Infra-only classifications (fallback for synthetic mode)
        if 'db_connections' in names:
            return 'db_saturation'
        if 'asg_capacity' in names:
            return 'asg_capacity_limit'
        if 'cpu_usage' in names or 'memory_usage' in names:
            return 'resource_exhaustion'

        return 'unknown_anomaly'

    def create_incident(self, service_name: str, anomalies: List[Dict]) -> Optional[int]:
        if not anomalies:
            return None

        df = pd.DataFrame(anomalies)
        # Use the earliest anomaly timestamp as the incident timestamp
        timestamp  = df.sort_values('timestamp')['timestamp'].iloc[0]
        inc_type   = self.classify_incident_type(anomalies)
        max_z      = df['zscore'].abs().max()

        if max_z >= 5:
            severity = 'critical'
        elif max_z >= 4:
            severity = 'high'
        elif max_z >= 3.5:
            severity = 'medium'
        else:
            severity = 'low'

        affected = df['metric_name'].unique().tolist()
        sources  = df['source'].unique().tolist()
        description = (
            f"Detected anomalous {inc_type.replace('_', ' ')} on {service_name}. "
            f"Affected metrics: {', '.join(affected)}. "
            f"Signal source(s): {', '.join(sources)}."
        )
        confidence = min(max_z / 6.0, 1.0)

        incident_id = self.db.insert_incident(
            timestamp=timestamp,
            service_name=service_name,
            incident_type=inc_type,
            severity=severity,
            description=description,
            affected_metrics=str(affected),
            confidence_score=round(confidence, 3),
        )
        logger.info(
            f"\u2713 Created incident #{incident_id}: {inc_type} "
            f"(severity={severity}, confidence={confidence:.2f})"
        )
        return incident_id


def main():
    logger.info("Starting anomaly detection")
    detector = AnomalyDetector()
    db = DatabaseManager()

    services = db.execute_query(
        "SELECT DISTINCT service_name FROM baselines ORDER BY service_name"
    )
    if not services:
        logger.error("No baselines found — run calculate_baselines.py first")
        return 1

    service_names = [s['service_name'] for s in services]
    logger.info(f"Checking {len(service_names)} services\n")

    # Determine whether we have real log metrics in the DB
    log_metric_check = db.execute_query(
        "SELECT COUNT(*) as n FROM metrics WHERE source='logs' LIMIT 1"
    )
    has_log_metrics = log_metric_check[0]['n'] > 0 if log_metric_check else False

    if has_log_metrics:
        logger.info("Real log metrics detected — using log-first detection mode")
    else:
        logger.info("No log metrics found — falling back to synthetic infra-metrics mode")

    total_anomalies = 0
    total_incidents = 0

    for svc in service_names:
        logger.info(f"Detecting anomalies for {svc}")

        if has_log_metrics:
            # Primary pass: log-derived metrics only
            log_anomalies = detector.detect_for_service(svc, source_filter='logs')

            if log_anomalies:
                total_anomalies += len(log_anomalies)
                inc_id = detector.create_incident(svc, log_anomalies)
                if inc_id:
                    total_incidents += 1
            else:
                logger.info(f"  No log anomalies for {svc}")
        else:
            # Fallback: synthetic infra metrics
            anomalies = detector.detect_for_service(svc)
            if anomalies:
                total_anomalies += len(anomalies)
                inc_id = detector.create_incident(svc, anomalies)
                if inc_id:
                    total_incidents += 1

    logger.info("\n=== DETECTION SUMMARY ===")
    logger.info(f"Total anomalies : {total_anomalies}")
    logger.info(f"Incidents created: {total_incidents}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Generate synthetic metrics with ground truth incidents."""
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.db_utils import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetricsGenerator:
    """Generate synthetic metrics for testing and evaluation."""

    SERVICES = [
        'Internal-core-ms',
        'External-core-ms-2',
        'User-ms-3',
        'Chargebee-EH-4'
    ]

    # Normal operating ranges
    METRIC_TYPES = {
        'cpu_usage':      {'min': 20,  'max': 60,  'unit': 'percent'},
        'memory_usage':   {'min': 40,  'max': 70,  'unit': 'percent'},
        'db_connections': {'min': 50,  'max': 150, 'unit': 'count'},
        'asg_capacity':   {'min': 2,   'max': 8,   'unit': 'instances'}
    }

    # Anomalous values — deliberately extreme so z-scores >> 3σ
    # even when baselines are calculated from contaminated data
    INCIDENT_VALUES = {
        'db_saturation': {
            'db_connections': (280, 320),   # normal max=150, will give z > 5
        },
        'asg_capacity': {
            'asg_capacity': (18, 22),        # normal max=8,   will give z > 7
            'cpu_usage':    (88, 96),         # normal max=60,  will give z > 3
            'memory_usage': (82, 92),         # normal max=70,  will give z > 3
        }
    }

    def __init__(self, start_time: datetime, duration_hours: int = 24):
        self.start_time = start_time
        self.duration_hours = duration_hours
        self.interval_minutes = 5

        num_points = (duration_hours * 60) // self.interval_minutes
        self.timestamps = [
            start_time + timedelta(minutes=i * self.interval_minutes)
            for i in range(num_points)
        ]

    def generate_normal_metrics(self, service: str) -> pd.DataFrame:
        """Generate normal baseline metrics for a service."""
        metrics = []
        for ts in self.timestamps:
            for metric_name, cfg in self.METRIC_TYPES.items():
                value = np.random.uniform(cfg['min'], cfg['max'])
                noise = np.random.normal(0, (cfg['max'] - cfg['min']) * 0.05)
                value = max(cfg['min'], min(cfg['max'], value + noise))
                metrics.append({
                    'timestamp':    ts.isoformat(),
                    'service_name': service,
                    'metric_name':  metric_name,
                    'metric_value': round(value, 2),
                    'metric_type':  metric_name,
                    'source':       'synthetic'
                })
        return pd.DataFrame(metrics)

    def inject_incident(
        self, df: pd.DataFrame, service: str,
        incident_start: datetime, incident_type: str,
        duration_minutes: int = 30
    ) -> pd.DataFrame:
        """Inject anomalous values into the metrics DataFrame.

        Values are set far outside the normal range so that z-score
        detection works even when baselines are calculated from the
        full 24-hour window (which includes this incident).
        """
        df = df.copy()
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
        incident_end = incident_start + timedelta(minutes=duration_minutes)

        base_mask = (
            (df['service_name'] == service) &
            (df['timestamp_dt'] >= incident_start) &
            (df['timestamp_dt'] <= incident_end)
        )

        overrides = self.INCIDENT_VALUES.get(incident_type, {})
        for metric_name, (lo, hi) in overrides.items():
            mask = base_mask & (df['metric_name'] == metric_name)
            count = mask.sum()
            if count > 0:
                df.loc[mask, 'metric_value'] = np.round(
                    np.random.uniform(lo, hi, size=count), 2
                )

        df = df.drop('timestamp_dt', axis=1)
        logger.info(
            f"Injected '{incident_type}' incident for {service} "
            f"at {incident_start} (metrics: {list(overrides.keys())})"
        )
        return df

    def generate_all_metrics(self) -> pd.DataFrame:
        """Generate metrics for all services with injected ground-truth incidents."""
        all_metrics = []
        for service in self.SERVICES:
            logger.info(f"Generating metrics for {service}")
            all_metrics.append(self.generate_normal_metrics(service))

        df_all = pd.concat(all_metrics, ignore_index=True)

        # Ground Truth Incident 1: DB saturation on External-core-ms-2 at T+6h
        df_all = self.inject_incident(
            df_all, 'External-core-ms-2',
            self.start_time + timedelta(hours=6),
            'db_saturation'
        )

        # Ground Truth Incident 2: ASG capacity limit on Internal-core-ms at T+12h
        df_all = self.inject_incident(
            df_all, 'Internal-core-ms',
            self.start_time + timedelta(hours=12),
            'asg_capacity'
        )

        return df_all


def main():
    """Generate synthetic metrics and persist to SQLite + CSV."""
    logger.info("Starting synthetic metrics generation")

    start_time = datetime(2024, 3, 1, 0, 0, 0)  # fixed seed for reproducibility
    generator  = MetricsGenerator(start_time, duration_hours=24)
    df_metrics = generator.generate_all_metrics()

    logger.info(f"Generated {len(df_metrics)} metric data points")

    db = DatabaseManager()
    logger.info("Inserting metrics into database...")

    now = datetime.now().isoformat()
    params_list = [
        (
            row['timestamp'], row['service_name'], row['metric_name'],
            row['metric_value'], row['metric_type'], row['source'], now
        )
        for _, row in df_metrics.iterrows()
    ]

    db.execute_many(
        """INSERT OR REPLACE INTO metrics
           (timestamp, service_name, metric_name, metric_value,
            metric_type, source, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        params_list
    )
    logger.info("✓ Metrics saved to database")

    metrics_dir = Path("data/metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)
    csv_path = metrics_dir / "synthetic_metrics.csv"
    df_metrics.to_csv(csv_path, index=False)
    logger.info(f"✓ Metrics saved to {csv_path}")

    logger.info("\n=== GROUND TRUTH INCIDENTS ===")
    logger.info("1. External-core-ms-2 — DB Saturation at T+6h")
    logger.info("   DB connections: 280–320  (normal: 50–150, z > 5σ)")
    logger.info("")
    logger.info("2. Internal-core-ms — ASG Capacity Limit at T+12h")
    logger.info("   ASG capacity:   18–22   (normal: 2–8,   z > 7σ)")
    logger.info("   CPU usage:      88–96%  (normal: 20–60%, z > 3σ)")
    logger.info("   Memory usage:   82–92%  (normal: 40–70%, z > 3σ)")
    logger.info("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
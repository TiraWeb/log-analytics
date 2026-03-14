"""Root cause diagnosis — two-signal model.

For each unresolved incident the diagnoser:
  1. Looks at which log metrics triggered the incident (error_rate, latency_p95, http_5xx_rate)
  2. Cross-correlates with infra metrics in the same time window
     (cpu_usage, memory_usage, asg_capacity, db_connections)
  3. Applies decision-tree rules to produce a human-readable root cause + action.

This matches the architecture described in the project interim (§4.1).
"""
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import numpy as np
from scipy import stats

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Decision-tree rule definitions
# Each rule maps a combination of elevated infra metrics → a diagnosis.
# Rules are evaluated in priority order (most specific first).
# ---------------------------------------------------------------------------
RULES = [
    {
        'name':        'infrastructure_saturation',
        'requires':    {'asg_capacity'},
        'description': 'ECS Auto Scaling Group at maximum capacity — new tasks cannot be placed',
        'action':      'Increase ASG max_size in the ECS capacity provider, or reduce task CPU/memory reservations',
        'scenario':    'A',  # TaskPlacementFailure scenario from interim §4.1
    },
    {
        'name':        'database_saturation',
        'requires':    {'db_connections'},
        'description': 'RDS connection pool approaching saturation',
        'action':      'Scale RDS instance vertically or increase max_connections; review connection pooling (PgBouncer)',
        'scenario':    'C',  # ConnectionRefused / dependency failure
    },
    {
        'name':        'resource_contention',
        'requires':    {'cpu_usage', 'memory_usage'},
        'description': 'High CPU and memory pressure — service is resource-constrained',
        'action':      'Scale ECS task definition (CPU/memory limits) or investigate memory leak',
        'scenario':    'B',  # High latency + resource pressure
    },
    {
        'name':        'cpu_pressure',
        'requires':    {'cpu_usage'},
        'description': 'CPU utilisation significantly above baseline',
        'action':      'Profile CPU-intensive code paths; consider horizontal scaling',
        'scenario':    'B',
    },
    {
        'name':        'memory_pressure',
        'requires':    {'memory_usage'},
        'description': 'Memory usage significantly above baseline — possible memory leak',
        'action':      'Restart container to reclaim memory; investigate heap growth',
        'scenario':    'B',
    },
    {
        'name':        'application_error',
        'requires':    set(),  # fallback — no infra signal needed
        'description': 'Elevated error rate with no correlated infrastructure anomaly',
        'action':      'Review application logs for exception stack traces; check recent deployments',
        'scenario':    'app',
    },
]


class RootCauseDiagnoser:

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.db = DatabaseManager(config_path)
        self.corr_threshold = config['analysis']['correlation_threshold']
        self.sigma          = config['analysis']['anomaly_threshold_sigma']

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def _get_incident(self, incident_id: int) -> Optional[Dict]:
        rows = self.db.execute_query(
            "SELECT * FROM incidents WHERE id = ?", (incident_id,)
        )
        return rows[0] if rows else None

    def _fetch_metrics_window(
        self, service_name: str, timestamp: str, window_minutes: int = 60
    ) -> pd.DataFrame:
        """Fetch all metrics in ±window_minutes around the incident."""
        query = """
            SELECT timestamp, metric_name, metric_value, source
            FROM   metrics
            WHERE  service_name = ?
            AND    timestamp >= strftime('%Y-%m-%dT%H:%M:%S',
                                         datetime(?, '-{w} minutes'))
            AND    timestamp <= strftime('%Y-%m-%dT%H:%M:%S',
                                         datetime(?, '+{w} minutes'))
            ORDER  BY timestamp
        """.format(w=window_minutes)
        rows = self.db.execute_query(query, (service_name, timestamp, timestamp))
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------

    def _elevated_infra_metrics(
        self, df: pd.DataFrame, service_name: str
    ) -> Dict[str, float]:
        """Return infra metric names whose mean in the window is above their baseline.

        Returns dict of {metric_name: z_score_of_window_mean}.
        """
        elevated = {}
        infra_df = df[df['source'].isin(['synthetic', 'cloudwatch'])].copy()
        if infra_df.empty:
            # Also check if any infra metrics are in the 'logs' source group (edge case)
            from src.analysis.detect_anomalies import INFRA_METRICS
            infra_df = df[df['metric_name'].isin(INFRA_METRICS)].copy()

        if infra_df.empty:
            return {}

        for metric_name, group in infra_df.groupby('metric_name'):
            baseline = self.db.execute_query(
                """SELECT mean, stddev FROM baselines
                   WHERE service_name = ? AND metric_name = ?
                   ORDER BY created_at DESC LIMIT 1""",
                (service_name, metric_name)
            )
            if not baseline or baseline[0]['stddev'] == 0:
                continue
            bm, bs = baseline[0]['mean'], baseline[0]['stddev']
            window_mean = group['metric_value'].mean()
            z = (window_mean - bm) / bs
            if abs(z) >= self.sigma:
                elevated[metric_name] = round(z, 2)

        return elevated

    def _match_rule(self, elevated_infra: Dict[str, float]) -> Dict:
        """Pick the highest-priority rule whose required metrics are all elevated."""
        elevated_set = set(elevated_infra.keys())
        for rule in RULES:
            if rule['requires'].issubset(elevated_set):
                return rule
        return RULES[-1]  # fallback: application_error

    def _correlated_log_metrics(
        self, df: pd.DataFrame, primary_infra: str
    ) -> List[str]:
        """Return log-derived metric names correlated with the primary infra metric."""
        pivot = df.pivot_table(
            index='timestamp', columns='metric_name', values='metric_value'
        )
        if primary_infra not in pivot.columns:
            return []
        corr = pivot.corr()[primary_infra].abs().drop(primary_infra, errors='ignore')
        return corr[corr >= self.corr_threshold].index.tolist()

    # ------------------------------------------------------------------
    # Main diagnosis
    # ------------------------------------------------------------------

    def diagnose_incident(self, incident_id: int) -> Optional[str]:
        incident = self._get_incident(incident_id)
        if not incident:
            logger.error(f"Incident #{incident_id} not found")
            return None

        logger.info(f"Diagnosing incident #{incident_id} "
                    f"({incident['incident_type']} on {incident['service_name']})")

        df = self._fetch_metrics_window(
            incident['service_name'], incident['timestamp']
        )
        if df.empty:
            logger.warning(f"  No metrics data in window for #{incident_id}")
            return None

        # Step 1: find elevated infra metrics in the window
        elevated = self._elevated_infra_metrics(df, incident['service_name'])
        logger.info(f"  Elevated infra metrics: {elevated or 'none'}")

        # Step 2: apply decision-tree rules
        rule = self._match_rule(elevated)
        logger.info(f"  Matched rule: {rule['name']} (Scenario {rule['scenario']})")

        # Step 3: build correlated metric list for display
        primary_infra = next(iter(rule['requires']), None)
        correlated = []
        if primary_infra:
            correlated = self._correlated_log_metrics(df, primary_infra)
        # Also include any other elevated infra metrics not already listed
        for m in elevated:
            if m != primary_infra and m not in correlated:
                correlated.append(m)

        corr_str    = ', '.join(correlated) if correlated else 'None'
        z_str       = ', '.join(f"{m}(z={z})" for m, z in elevated.items()) if elevated else 'None'
        root_cause  = (
            f"{rule['description']}. "
            f"Primary metric: {primary_infra or 'error_rate'}. "
            f"Correlated metrics: {corr_str}. "
            f"Recommended action: {rule['action']}"
        )

        logger.info(f"  \u2713 Root cause : {rule['name']}")
        logger.info(f"    Description : {rule['description']}")
        logger.info(f"    Elevated    : {z_str}")
        logger.info(f"    Correlated  : {corr_str}")
        logger.info(f"    Action      : {rule['action']}")

        self.db.execute_insert(
            "UPDATE incidents SET root_cause = ? WHERE id = ?",
            (root_cause, incident_id)
        )
        return root_cause


def main():
    logger.info("Starting root cause diagnosis")
    diagnoser = RootCauseDiagnoser()
    db        = DatabaseManager()

    incidents = db.execute_query(
        """SELECT id, service_name, incident_type, severity
           FROM   incidents
           WHERE  resolved = 0 AND root_cause IS NULL
           ORDER  BY timestamp DESC"""
    )
    if not incidents:
        logger.info("No incidents to diagnose")
        return 0

    logger.info(f"Found {len(incidents)} incidents to diagnose\n")
    diagnosed = 0
    for inc in incidents:
        rc = diagnoser.diagnose_incident(inc['id'])
        if rc:
            diagnosed += 1
        print()

    logger.info("=== DIAGNOSIS SUMMARY ===")
    logger.info(f"Total      : {len(incidents)}")
    logger.info(f"Diagnosed  : {diagnosed}")
    logger.info(f"Unresolved : {len(incidents) - diagnosed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

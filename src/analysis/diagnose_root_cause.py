"""Root cause diagnosis using correlation analysis and pattern matching."""
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from scipy import stats

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.db_utils import DatabaseManager
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RootCauseDiagnoser:
    """Diagnose root causes of incidents using correlation and patterns."""

    PATTERNS = {
        'db_saturation': {
            'primary_metric': 'db_connections',
            'description': 'Database connection pool approaching saturation',
            'remediation': 'Scale RDS instance or increase connection pool size'
        },
        'asg_capacity_limit': {
            'primary_metric': 'asg_capacity',
            'correlated_metrics': ['cpu_usage', 'memory_usage'],
            'description': 'Auto Scaling Group at maximum capacity',
            'remediation': 'Increase ASG max_size or optimise resource usage'
        },
        'resource_exhaustion': {
            'primary_metric': 'cpu_usage',
            'description': 'CPU / memory exhaustion',
            'remediation': 'Scale service or investigate resource-intensive operations'
        }
    }

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.db = DatabaseManager(config_path)
        self.correlation_threshold = config['analysis']['correlation_threshold']

    # ------------------------------------------------------------------
    # BUG FIX: SQLite's datetime() returns "YYYY-MM-DD HH:MM:SS" (space)
    # but our stored timestamps use the ISO-8601 'T' separator.
    # Wrapping with strftime ensures both sides of BETWEEN use the same
    # format, so string comparison works correctly.
    # ------------------------------------------------------------------
    def get_incident_metrics(self, incident_id: int, window_minutes: int = 60) -> pd.DataFrame:
        """Fetch all metrics within ±window_minutes of the incident timestamp."""
        rows = self.db.execute_query(
            "SELECT * FROM incidents WHERE id = ?", (incident_id,)
        )
        if not rows:
            logger.error(f"Incident {incident_id} not found")
            return pd.DataFrame()

        incident = rows[0]

        query = """
            SELECT timestamp, metric_name, metric_value
            FROM   metrics
            WHERE  service_name = ?
            AND    timestamp >= strftime('%Y-%m-%dT%H:%M:%S',
                                         datetime(?, '-{w} minutes'))
            AND    timestamp <= strftime('%Y-%m-%dT%H:%M:%S',
                                         datetime(?, '+{w} minutes'))
            ORDER  BY timestamp
        """.format(w=window_minutes)

        results = self.db.execute_query(
            query,
            (incident['service_name'], incident['timestamp'], incident['timestamp'])
        )

        if not results:
            logger.warning(
                f"No metrics in \u00b1{window_minutes} min window for incident "
                f"#{incident_id} (service={incident['service_name']}, "
                f"ts={incident['timestamp']})"
            )
            return pd.DataFrame()

        return pd.DataFrame(results)

    def calculate_correlation_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        pivot = df.pivot_table(
            index='timestamp', columns='metric_name', values='metric_value'
        )
        return pivot.corr()

    def identify_correlated_metrics(self, df: pd.DataFrame, primary_metric: str) -> List[str]:
        if df.empty or primary_metric not in df['metric_name'].values:
            return []
        corr = self.calculate_correlation_matrix(df)
        if primary_metric not in corr.columns:
            return []
        correlations = corr[primary_metric].abs()
        return correlations[
            (correlations >= self.correlation_threshold) &
            (correlations.index != primary_metric)
        ].index.tolist()

    def match_pattern(self, incident: Dict, df: pd.DataFrame) -> Optional[Dict]:
        incident_type = incident['incident_type']
        if incident_type not in self.PATTERNS:
            return None

        pattern = self.PATTERNS[incident_type]
        primary = pattern['primary_metric']

        if primary not in df['metric_name'].values:
            # Try a fuzzy match: pick the pattern whose primary metric
            # is actually present in the data
            for pname, pdef in self.PATTERNS.items():
                if pdef['primary_metric'] in df['metric_name'].values:
                    pattern = pdef
                    primary = pdef['primary_metric']
                    incident_type = pname
                    break
            else:
                return None

        correlated = self.identify_correlated_metrics(df, primary)
        return {
            'pattern_name':       incident_type,
            'primary_metric':     primary,
            'correlated_metrics': correlated,
            'description':        pattern['description'],
            'remediation':        pattern['remediation']
        }

    def diagnose_incident(self, incident_id: int) -> Optional[str]:
        logger.info(f"Diagnosing incident #{incident_id}")

        incidents = self.db.execute_query(
            "SELECT * FROM incidents WHERE id = ?", (incident_id,)
        )
        if not incidents:
            logger.error(f"Incident {incident_id} not found")
            return None

        incident = incidents[0]
        df = self.get_incident_metrics(incident_id)

        if df.empty:
            return None

        matched = self.match_pattern(incident, df)
        if not matched:
            logger.warning(f"  No pattern match for incident #{incident_id}")
            return None

        corr_str = ', '.join(matched['correlated_metrics']) or 'None'
        root_cause = (
            f"{matched['description']}. "
            f"Primary metric: {matched['primary_metric']}. "
            f"Correlated metrics: {corr_str}. "
            f"Recommended action: {matched['remediation']}"
        )

        logger.info(f"  ✓ Root cause: {matched['pattern_name']}")
        logger.info(f"    {matched['description']}")
        logger.info(f"    Correlated: {corr_str}")
        logger.info(f"    Fix: {matched['remediation']}")

        self.db.execute_insert(
            "UPDATE incidents SET root_cause = ? WHERE id = ?",
            (root_cause, incident_id)
        )
        return root_cause


def main():
    """Diagnose all unresolved incidents without a root cause."""
    logger.info("Starting root cause diagnosis")

    diagnoser = RootCauseDiagnoser()
    db = DatabaseManager()

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

    logger.info("\n=== DIAGNOSIS SUMMARY ===")
    logger.info(f"Total:      {len(incidents)}")
    logger.info(f"Diagnosed:  {diagnosed}")
    logger.info(f"Unresolved: {len(incidents) - diagnosed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
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
    
    # Known patterns
    PATTERNS = {
        'db_saturation': {
            'primary_metric': 'db_connections',
            'threshold': 0.9,  # 90% of max capacity
            'description': 'Database connection pool approaching saturation',
            'remediation': 'Scale RDS instance or increase connection pool size'
        },
        'asg_capacity_limit': {
            'primary_metric': 'asg_capacity',
            'correlated_metrics': ['cpu_usage', 'memory_usage'],
            'description': 'Auto Scaling Group at maximum capacity',
            'remediation': 'Increase ASG max_size or optimize application resource usage'
        },
        'resource_exhaustion': {
            'primary_metric': 'cpu_usage',
            'threshold': 0.85,
            'description': 'CPU or memory exhaustion',
            'remediation': 'Scale service or investigate resource-intensive operations'
        }
    }
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize root cause diagnoser.
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.db = DatabaseManager(config_path)
        self.correlation_threshold = config['analysis']['correlation_threshold']
    
    def get_incident_metrics(self, incident_id: int, window_minutes: int = 30) -> pd.DataFrame:
        """Get metrics around incident time.
        
        Args:
            incident_id: Incident ID
            window_minutes: Time window around incident
            
        Returns:
            DataFrame with metrics
        """
        # Get incident details
        incident = self.db.execute_query(
            "SELECT * FROM incidents WHERE id = ?",
            (incident_id,)
        )[0]
        
        # Get metrics around incident timestamp
        query = """SELECT timestamp, metric_name, metric_value
                   FROM metrics
                   WHERE service_name = ?
                   AND timestamp BETWEEN datetime(?, '-30 minutes') AND datetime(?, '+30 minutes')
                   ORDER BY timestamp"""
        
        results = self.db.execute_query(
            query,
            (incident['service_name'], incident['timestamp'], incident['timestamp'])
        )
        
        if not results:
            return pd.DataFrame()
        
        return pd.DataFrame(results)
    
    def calculate_correlation_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate correlation between metrics.
        
        Args:
            df: Metrics DataFrame
            
        Returns:
            Correlation matrix
        """
        # Pivot to wide format
        pivot = df.pivot_table(
            index='timestamp',
            columns='metric_name',
            values='metric_value'
        )
        
        # Calculate correlation
        return pivot.corr()
    
    def identify_correlated_metrics(self, df: pd.DataFrame, primary_metric: str) -> List[str]:
        """Identify metrics correlated with primary metric.
        
        Args:
            df: Metrics DataFrame
            primary_metric: Primary anomalous metric
            
        Returns:
            List of correlated metric names
        """
        if df.empty or primary_metric not in df['metric_name'].values:
            return []
        
        corr_matrix = self.calculate_correlation_matrix(df)
        
        if primary_metric not in corr_matrix.columns:
            return []
        
        # Find highly correlated metrics
        correlations = corr_matrix[primary_metric].abs()
        correlated = correlations[
            (correlations >= self.correlation_threshold) &
            (correlations.index != primary_metric)
        ]
        
        return correlated.index.tolist()
    
    def match_pattern(self, incident: Dict, df: pd.DataFrame) -> Optional[Dict]:
        """Match incident to known patterns.
        
        Args:
            incident: Incident dictionary
            df: Metrics DataFrame
            
        Returns:
            Matched pattern dictionary or None
        """
        incident_type = incident['incident_type']
        
        if incident_type in self.PATTERNS:
            pattern = self.PATTERNS[incident_type]
            
            # Verify primary metric is present
            primary_metric = pattern['primary_metric']
            
            if primary_metric in df['metric_name'].values:
                # Check if correlated metrics are present
                correlated = []
                if 'correlated_metrics' in pattern:
                    correlated = self.identify_correlated_metrics(df, primary_metric)
                
                return {
                    'pattern_name': incident_type,
                    'primary_metric': primary_metric,
                    'correlated_metrics': correlated,
                    'description': pattern['description'],
                    'remediation': pattern['remediation']
                }
        
        return None
    
    def diagnose_incident(self, incident_id: int) -> Optional[str]:
        """Diagnose root cause of incident.
        
        Args:
            incident_id: Incident ID
            
        Returns:
            Root cause description or None
        """
        logger.info(f"Diagnosing incident #{incident_id}")
        
        # Get incident
        incident = self.db.execute_query(
            "SELECT * FROM incidents WHERE id = ?",
            (incident_id,)
        )
        
        if not incident:
            logger.error(f"Incident {incident_id} not found")
            return None
        
        incident = incident[0]
        
        # Get metrics around incident
        df = self.get_incident_metrics(incident_id)
        
        if df.empty:
            logger.warning(f"No metrics found for incident {incident_id}")
            return None
        
        # Match to known patterns
        matched_pattern = self.match_pattern(incident, df)
        
        if matched_pattern:
            root_cause = (
                f"{matched_pattern['description']}. "
                f"Primary metric: {matched_pattern['primary_metric']}. "
                f"Correlated metrics: {', '.join(matched_pattern['correlated_metrics']) if matched_pattern['correlated_metrics'] else 'None'}. "
                f"Recommended action: {matched_pattern['remediation']}"
            )
            
            logger.info(f"  ✓ Root cause identified: {matched_pattern['pattern_name']}")
            logger.info(f"  Description: {matched_pattern['description']}")
            logger.info(f"  Remediation: {matched_pattern['remediation']}")
            
            # Update incident with root cause
            self.db.execute_insert(
                "UPDATE incidents SET root_cause = ? WHERE id = ?",
                (root_cause, incident_id)
            )
            
            return root_cause
        else:
            logger.warning(f"  No pattern match for incident {incident_id}")
            return None


def main():
    """Diagnose all unresolved incidents."""
    logger.info("Starting root cause diagnosis")
    
    diagnoser = RootCauseDiagnoser()
    db = DatabaseManager()
    
    # Get unresolved incidents without root cause
    incidents = db.execute_query(
        """SELECT id, service_name, incident_type, severity
           FROM incidents
           WHERE resolved = 0 AND root_cause IS NULL
           ORDER BY timestamp DESC"""
    )
    
    if not incidents:
        logger.info("No incidents to diagnose")
        return 0
    
    logger.info(f"Found {len(incidents)} incidents to diagnose\n")
    
    diagnosed = 0
    
    for incident in incidents:
        root_cause = diagnoser.diagnose_incident(incident['id'])
        if root_cause:
            diagnosed += 1
        print()  # Blank line between incidents
    
    logger.info(f"\n=== DIAGNOSIS SUMMARY ===")
    logger.info(f"Total incidents: {len(incidents)}")
    logger.info(f"Successfully diagnosed: {diagnosed}")
    logger.info(f"Unable to diagnose: {len(incidents) - diagnosed}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
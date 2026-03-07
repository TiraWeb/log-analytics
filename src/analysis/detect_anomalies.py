"""Detect anomalies using statistical methods."""
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.db_utils import DatabaseManager
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detect anomalies using Z-score and statistical methods."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize anomaly detector.
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.db = DatabaseManager(config_path)
        self.threshold_sigma = config['analysis']['anomaly_threshold_sigma']
    
    def get_baseline(self, service_name: str, metric_name: str) -> Optional[Dict]:
        """Get baseline for a specific metric.
        
        Args:
            service_name: Service name
            metric_name: Metric name
            
        Returns:
            Baseline dictionary or None
        """
        query = """SELECT * FROM baselines
                   WHERE service_name = ? AND metric_name = ?
                   ORDER BY created_at DESC
                   LIMIT 1"""
        
        results = self.db.execute_query(query, (service_name, metric_name))
        return results[0] if results else None
    
    def calculate_zscore(self, value: float, baseline: Dict) -> float:
        """Calculate Z-score for a value.
        
        Args:
            value: Current value
            baseline: Baseline statistics
            
        Returns:
            Z-score
        """
        if baseline['stddev'] == 0:
            return 0.0
        
        return (value - baseline['mean']) / baseline['stddev']
    
    def detect_metric_anomalies(self, service_name: str) -> List[Dict]:
        """Detect anomalies in infrastructure metrics.
        
        Args:
            service_name: Service to check
            
        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting anomalies for {service_name}")
        
        # Get recent metrics
        query = """SELECT timestamp, metric_name, metric_value
                   FROM metrics
                   WHERE service_name = ?
                   ORDER BY timestamp DESC
                   LIMIT 1000"""
        
        results = self.db.execute_query(query, (service_name,))
        
        if not results:
            logger.warning(f"No metrics found for {service_name}")
            return []
        
        df = pd.DataFrame(results)
        anomalies = []
        
        for metric_name in df['metric_name'].unique():
            baseline = self.get_baseline(service_name, metric_name)
            
            if not baseline:
                logger.debug(f"No baseline for {service_name}/{metric_name}")
                continue
            
            metric_df = df[df['metric_name'] == metric_name]
            
            for _, row in metric_df.iterrows():
                zscore = self.calculate_zscore(row['metric_value'], baseline)
                
                if abs(zscore) >= self.threshold_sigma:
                    severity = 'high' if abs(zscore) >= 4 else 'medium'
                    
                    anomaly = {
                        'timestamp': row['timestamp'],
                        'service_name': service_name,
                        'metric_name': metric_name,
                        'value': row['metric_value'],
                        'baseline_mean': baseline['mean'],
                        'baseline_p95': baseline['p95'],
                        'zscore': zscore,
                        'severity': severity
                    }
                    
                    anomalies.append(anomaly)
                    
                    logger.info(
                        f"  ⚠  {metric_name}: {row['metric_value']:.2f} "
                        f"(baseline: {baseline['mean']:.2f}, z-score: {zscore:.2f})"
                    )
        
        return anomalies
    
    def classify_incident_type(self, anomalies: List[Dict]) -> str:
        """Classify type of incident based on anomalies.
        
        Args:
            anomalies: List of anomalies
            
        Returns:
            Incident type
        """
        metric_names = {a['metric_name'] for a in anomalies}
        
        if 'db_connections' in metric_names:
            return 'db_saturation'
        elif 'asg_capacity' in metric_names:
            return 'asg_capacity_limit'
        elif 'cpu_usage' in metric_names or 'memory_usage' in metric_names:
            return 'resource_exhaustion'
        else:
            return 'unknown_anomaly'
    
    def create_incident(self, service_name: str, anomalies: List[Dict]) -> Optional[int]:
        """Create incident from detected anomalies.
        
        Args:
            service_name: Service name
            anomalies: List of anomalies
            
        Returns:
            Incident ID or None
        """
        if not anomalies:
            return None
        
        # Group anomalies by timestamp
        df = pd.DataFrame(anomalies)
        timestamp = df['timestamp'].iloc[0]
        
        incident_type = self.classify_incident_type(anomalies)
        
        # Determine severity
        max_zscore = df['zscore'].abs().max()
        if max_zscore >= 4:
            severity = 'critical'
        elif max_zscore >= 3.5:
            severity = 'high'
        else:
            severity = 'medium'
        
        # Create description
        affected_metrics = df['metric_name'].unique().tolist()
        description = f"Detected {len(anomalies)} anomalous metrics: {', '.join(affected_metrics)}"
        
        # Calculate confidence
        confidence = min(max_zscore / 5.0, 1.0)
        
        incident_id = self.db.insert_incident(
            timestamp=timestamp,
            service_name=service_name,
            incident_type=incident_type,
            severity=severity,
            description=description,
            affected_metrics=str(affected_metrics),
            confidence_score=confidence
        )
        
        logger.info(
            f"✓ Created incident #{incident_id}: {incident_type} "
            f"(severity: {severity}, confidence: {confidence:.2f})"
        )
        
        return incident_id


def main():
    """Detect anomalies and create incidents."""
    logger.info("Starting anomaly detection")
    
    detector = AnomalyDetector()
    db = DatabaseManager()
    
    # Get all services with baselines
    services = db.execute_query("SELECT DISTINCT service_name FROM baselines ORDER BY service_name")
    
    if not services:
        logger.error("No baselines found. Run calculate_baselines.py first")
        return 1
    
    service_names = [s['service_name'] for s in services]
    logger.info(f"Checking {len(service_names)} services for anomalies\n")
    
    total_anomalies = 0
    total_incidents = 0
    
    for service_name in service_names:
        anomalies = detector.detect_metric_anomalies(service_name)
        
        if anomalies:
            total_anomalies += len(anomalies)
            incident_id = detector.create_incident(service_name, anomalies)
            if incident_id:
                total_incidents += 1
    
    logger.info(f"\n=== DETECTION SUMMARY ===")
    logger.info(f"Total anomalies detected: {total_anomalies}")
    logger.info(f"Incidents created: {total_incidents}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
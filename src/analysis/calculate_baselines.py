"""Calculate statistical baselines for normal service behavior."""
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.db_utils import DatabaseManager
from src.utils.openobserve_client import OpenObserveClient
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BaselineCalculator:
    """Calculate statistical baselines from historical data."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize baseline calculator.
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.db = DatabaseManager(config_path)
        self.client = OpenObserveClient(config_path)
        self.window_hours = config['analysis']['baseline_window_hours']
        self.min_samples = config['analysis']['min_samples_for_baseline']
    
    def calculate_metrics_baseline(self, service_name: str) -> List[Dict]:
        """Calculate baselines for infrastructure metrics.
        
        Args:
            service_name: Service to calculate baselines for
            
        Returns:
            List of baseline dictionaries
        """
        logger.info(f"Calculating metrics baselines for {service_name}")
        
        # Get metrics from database
        query = """SELECT timestamp, metric_name, metric_value
                   FROM metrics
                   WHERE service_name = ?
                   ORDER BY timestamp"""
        
        results = self.db.execute_query(query, (service_name,))
        
        if not results:
            logger.warning(f"No metrics found for {service_name}")
            return []
        
        df = pd.DataFrame(results)
        
        if len(df) < self.min_samples:
            logger.warning(f"Insufficient samples for {service_name}: {len(df)} < {self.min_samples}")
            return []
        
        baselines = []
        
        # Calculate baseline for each metric type
        for metric_name in df['metric_name'].unique():
            metric_df = df[df['metric_name'] == metric_name]
            values = metric_df['metric_value'].values
            
            if len(values) < 10:
                continue
            
            baseline = {
                'service_name': service_name,
                'metric_name': metric_name,
                'mean': float(np.mean(values)),
                'stddev': float(np.std(values)),
                'p50': float(np.percentile(values, 50)),
                'p95': float(np.percentile(values, 95)),
                'p99': float(np.percentile(values, 99)),
                'sample_count': len(values),
                'window_start': metric_df['timestamp'].min(),
                'window_end': metric_df['timestamp'].max()
            }
            
            baselines.append(baseline)
            
            logger.info(
                f"  {metric_name}: mean={baseline['mean']:.2f}, "
                f"stddev={baseline['stddev']:.2f}, p95={baseline['p95']:.2f}"
            )
        
        return baselines
    
    def calculate_log_baselines(self, service_name: str) -> List[Dict]:
        """Calculate baselines for log-based metrics.
        
        Args:
            service_name: Service to calculate baselines for
            
        Returns:
            List of baseline dictionaries
        """
        logger.info(f"Calculating log baselines for {service_name}")
        
        baselines = []
        
        # Error rate baseline
        sql_error_rate = f"""
        SELECT 
            timestamp_dt,
            is_error,
            COUNT(*) as log_count
        FROM {self.client.stream_name}
        WHERE service_name = '{service_name}'
        GROUP BY timestamp_dt, is_error
        ORDER BY timestamp_dt
        """
        
        error_results = self.client.execute_sql(sql_error_rate)
        
        if error_results:
            df_errors = pd.DataFrame(error_results)
            
            # Calculate error rate per time bucket
            error_rates = []
            for ts in df_errors['timestamp_dt'].unique():
                ts_df = df_errors[df_errors['timestamp_dt'] == ts]
                total = ts_df['log_count'].sum()
                errors = ts_df[ts_df['is_error'] == True]['log_count'].sum() if True in ts_df['is_error'].values else 0
                error_rate = (errors / total * 100) if total > 0 else 0
                error_rates.append(error_rate)
            
            if error_rates:
                baseline = {
                    'service_name': service_name,
                    'metric_name': 'error_rate',
                    'mean': float(np.mean(error_rates)),
                    'stddev': float(np.std(error_rates)),
                    'p50': float(np.percentile(error_rates, 50)),
                    'p95': float(np.percentile(error_rates, 95)),
                    'p99': float(np.percentile(error_rates, 99)),
                    'sample_count': len(error_rates),
                    'window_start': df_errors['timestamp_dt'].min(),
                    'window_end': df_errors['timestamp_dt'].max()
                }
                
                baselines.append(baseline)
                
                logger.info(
                    f"  error_rate: mean={baseline['mean']:.2f}%, "
                    f"stddev={baseline['stddev']:.2f}, p95={baseline['p95']:.2f}%"
                )
        
        # Latency baseline
        sql_latency = f"""
        SELECT latency_ms
        FROM {self.client.stream_name}
        WHERE service_name = '{service_name}'
        AND latency_ms IS NOT NULL
        """
        
        latency_results = self.client.execute_sql(sql_latency)
        
        if latency_results:
            latencies = [r['latency_ms'] for r in latency_results]
            
            if latencies:
                baseline = {
                    'service_name': service_name,
                    'metric_name': 'latency_p95',
                    'mean': float(np.mean(latencies)),
                    'stddev': float(np.std(latencies)),
                    'p50': float(np.percentile(latencies, 50)),
                    'p95': float(np.percentile(latencies, 95)),
                    'p99': float(np.percentile(latencies, 99)),
                    'sample_count': len(latencies),
                    'window_start': '',  # From log query
                    'window_end': ''
                }
                
                baselines.append(baseline)
                
                logger.info(
                    f"  latency_p95: mean={baseline['mean']:.2f}ms, "
                    f"stddev={baseline['stddev']:.2f}, p95={baseline['p95']:.2f}ms"
                )
        
        return baselines
    
    def save_baselines(self, baselines: List[Dict]):
        """Save baselines to database.
        
        Args:
            baselines: List of baseline dictionaries
        """
        for baseline in baselines:
            self.db.insert_baseline(
                service_name=baseline['service_name'],
                metric_name=baseline['metric_name'],
                mean=baseline['mean'],
                stddev=baseline['stddev'],
                p50=baseline['p50'],
                p95=baseline['p95'],
                p99=baseline['p99'],
                sample_count=baseline['sample_count'],
                window_start=baseline['window_start'],
                window_end=baseline['window_end']
            )
        
        logger.info(f"Saved {len(baselines)} baselines to database")


def main():
    """Calculate and save baselines for all services."""
    logger.info("Starting baseline calculation")
    
    calculator = BaselineCalculator()
    
    # Get list of services from metrics
    db = DatabaseManager()
    services = db.execute_query("SELECT DISTINCT service_name FROM metrics ORDER BY service_name")
    
    if not services:
        logger.warning("No services found in metrics table")
        logger.info("Run generate_metrics.py first to create synthetic metrics")
        return 1
    
    service_names = [s['service_name'] for s in services]
    logger.info(f"Found {len(service_names)} services: {', '.join(service_names)}")
    
    all_baselines = []
    
    for service_name in service_names:
        # Calculate metrics baselines
        metrics_baselines = calculator.calculate_metrics_baseline(service_name)
        all_baselines.extend(metrics_baselines)
        
        # Calculate log baselines (if OpenObserve has data)
        # Uncomment after running ingest_logs.py
        # log_baselines = calculator.calculate_log_baselines(service_name)
        # all_baselines.extend(log_baselines)
    
    # Save all baselines
    if all_baselines:
        calculator.save_baselines(all_baselines)
        logger.info(f"\n✓ Calculated and saved {len(all_baselines)} baselines")
    else:
        logger.warning("No baselines calculated")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
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
    
    # Services to generate metrics for
    SERVICES = [
        'Internal-core-ms',
        'External-core-ms-2',
        'User-ms-3',
        'Chargebee-EH-4'
    ]
    
    # Metric types
    METRIC_TYPES = {
        'cpu_usage': {'min': 20, 'max': 60, 'unit': 'percent'},
        'memory_usage': {'min': 40, 'max': 70, 'unit': 'percent'},
        'db_connections': {'min': 50, 'max': 150, 'unit': 'count'},
        'asg_capacity': {'min': 2, 'max': 8, 'unit': 'instances'}
    }
    
    def __init__(self, start_time: datetime, duration_hours: int = 24):
        """Initialize metrics generator.
        
        Args:
            start_time: Start timestamp for metrics
            duration_hours: Duration to generate metrics for
        """
        self.start_time = start_time
        self.duration_hours = duration_hours
        self.interval_minutes = 5
        
        # Calculate time points
        num_points = (duration_hours * 60) // self.interval_minutes
        self.timestamps = [
            start_time + timedelta(minutes=i * self.interval_minutes)
            for i in range(num_points)
        ]
    
    def generate_normal_metrics(self, service: str) -> pd.DataFrame:
        """Generate normal baseline metrics.
        
        Args:
            service: Service name
            
        Returns:
            DataFrame with normal metrics
        """
        metrics = []
        
        for ts in self.timestamps:
            for metric_name, config in self.METRIC_TYPES.items():
                # Generate random value within normal range
                value = np.random.uniform(config['min'], config['max'])
                
                # Add some noise
                noise = np.random.normal(0, (config['max'] - config['min']) * 0.05)
                value = max(config['min'], min(config['max'], value + noise))
                
                metrics.append({
                    'timestamp': ts.isoformat(),
                    'service_name': service,
                    'metric_name': metric_name,
                    'metric_value': round(value, 2),
                    'metric_type': metric_name,
                    'source': 'synthetic'
                })
        
        return pd.DataFrame(metrics)
    
    def inject_incident(self, df: pd.DataFrame, service: str, 
                       incident_start: datetime, incident_type: str) -> pd.DataFrame:
        """Inject incident anomaly into metrics.
        
        Args:
            df: Metrics DataFrame
            service: Service name
            incident_start: Incident start time
            incident_type: Type of incident ('db_saturation' or 'asg_capacity')
            
        Returns:
            DataFrame with injected anomaly
        """
        df = df.copy()
        
        # Convert timestamps to datetime
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
        
        # Define incident window (30 minutes)
        incident_end = incident_start + timedelta(minutes=30)
        
        # Filter to incident window
        mask = (
            (df['service_name'] == service) &
            (df['timestamp_dt'] >= incident_start) &
            (df['timestamp_dt'] <= incident_end)
        )
        
        if incident_type == 'db_saturation':
            # Spike DB connections to 95% of max (approaching saturation)
            db_mask = mask & (df['metric_name'] == 'db_connections')
            df.loc[db_mask, 'metric_value'] = np.random.uniform(190, 200)
            
            logger.info(f"Injected DB saturation incident for {service} at {incident_start}")
        
        elif incident_type == 'asg_capacity':
            # Max out ASG capacity
            asg_mask = mask & (df['metric_name'] == 'asg_capacity')
            df.loc[asg_mask, 'metric_value'] = 10  # Max capacity hit
            
            # Also spike CPU and memory
            cpu_mask = mask & (df['metric_name'] == 'cpu_usage')
            df.loc[cpu_mask, 'metric_value'] = np.random.uniform(85, 95)
            
            mem_mask = mask & (df['metric_name'] == 'memory_usage')
            df.loc[mem_mask, 'metric_value'] = np.random.uniform(80, 90)
            
            logger.info(f"Injected ASG capacity incident for {service} at {incident_start}")
        
        # Drop helper column
        df = df.drop('timestamp_dt', axis=1)
        
        return df
    
    def generate_all_metrics(self) -> pd.DataFrame:
        """Generate metrics for all services with injected incidents.
        
        Returns:
            DataFrame with all metrics
        """
        all_metrics = []
        
        for service in self.SERVICES:
            logger.info(f"Generating metrics for {service}")
            df_service = self.generate_normal_metrics(service)
            all_metrics.append(df_service)
        
        # Combine all services
        df_all = pd.concat(all_metrics, ignore_index=True)
        
        # Inject Ground Truth incidents
        incident_1_time = self.start_time + timedelta(hours=6)
        df_all = self.inject_incident(df_all, 'External-core-ms-2', incident_1_time, 'db_saturation')
        
        incident_2_time = self.start_time + timedelta(hours=12)
        df_all = self.inject_incident(df_all, 'Internal-core-ms', incident_2_time, 'asg_capacity')
        
        return df_all


def main():
    """Generate synthetic metrics and save to database."""
    logger.info("Starting synthetic metrics generation")
    
    # Use a fixed start time for reproducibility
    start_time = datetime(2024, 3, 1, 0, 0, 0)
    
    # Generate metrics
    generator = MetricsGenerator(start_time, duration_hours=24)
    df_metrics = generator.generate_all_metrics()
    
    logger.info(f"Generated {len(df_metrics)} metric data points")
    
    # Save to database
    db = DatabaseManager()
    
    logger.info("Inserting metrics into database...")
    
    params_list = [
        (
            row['timestamp'],
            row['service_name'],
            row['metric_name'],
            row['metric_value'],
            row['metric_type'],
            row['source']
        )
        for _, row in df_metrics.iterrows()
    ]
    
    query = """INSERT OR REPLACE INTO metrics
               (timestamp, service_name, metric_name, metric_value, metric_type, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)"""
    
    # Add created_at timestamp to each tuple
    params_list = [params + (datetime.now().isoformat(),) for params in params_list]
    
    db.execute_many(query, params_list)
    
    logger.info("✓ Metrics saved to database")
    
    # Also save to CSV for reference
    metrics_dir = Path("data/metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = metrics_dir / "synthetic_metrics.csv"
    df_metrics.to_csv(csv_path, index=False)
    
    logger.info(f"✓ Metrics saved to {csv_path}")
    
    # Print summary
    logger.info("\n=== GROUND TRUTH INCIDENTS ===")
    logger.info("1. External-core-ms-2: DB Saturation at T+6h")
    logger.info("   - Duration: 30 minutes")
    logger.info("   - DB connections spike to 190-200 (normal: 50-150)")
    logger.info("")
    logger.info("2. Internal-core-ms: ASG Capacity Limit at T+12h")
    logger.info("   - Duration: 30 minutes")
    logger.info("   - ASG capacity: 10 instances (max)")
    logger.info("   - CPU usage: 85-95% (normal: 20-60%)")
    logger.info("   - Memory usage: 80-90% (normal: 40-70%)")
    logger.info("="*50)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
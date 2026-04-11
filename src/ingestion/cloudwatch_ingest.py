"""CloudWatch metrics ingestion — pulls real AWS infra metrics into SQLite.

Requires boto3 + AWS credentials (any standard method works:
  - env vars: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
  - ~/.aws/credentials
  - IAM instance profile / ECS task role)

Metrics pulled per ECS service:
  - CPUUtilization        → cpu_usage
  - MemoryUtilization     → memory_usage
  - RunningTaskCount      → asg_capacity   (ECS service desired count proxy)

Metrics pulled per RDS cluster (optional, set RDS_CLUSTER_ID in config):
  - DatabaseConnections   → db_connections
  - FreeableMemory        → rds_memory_mb
  - CPUUtilization        → rds_cpu

Usage:
    python src/ingestion/cloudwatch_ingest.py
    python src/ingestion/cloudwatch_ingest.py --hours 48
    python src/ingestion/cloudwatch_ingest.py --hours 12 --region ap-southeast-1
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import yaml

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ── Name maps ─────────────────────────────────────────────────────────────────
# Maps CloudWatch metric name → our internal metric_name
ECS_METRIC_MAP = {
    'CPUUtilization':    'cpu_usage',
    'MemoryUtilization': 'memory_usage',
    'RunningTaskCount':  'asg_capacity',
}

RDS_METRIC_MAP = {
    'DatabaseConnections': 'db_connections',
    'CPUUtilization':      'rds_cpu',
    'FreeableMemory':      'rds_memory_mb',   # bytes → MB on ingest
}

ALB_METRIC_MAP = {
    'HTTPCode_Target_5XX_Count': 'http_5xx_count',
    'TargetResponseTime':        'latency_p95',   # approximated via Average
    'RequestCount':              'request_count',
}


class CloudWatchIngestor:
    """Pull metrics from CloudWatch and persist to SQLite."""

    def __init__(self, config_path: str = 'config/config.yaml',
                 region: Optional[str] = None, hours: int = 24):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        self.db         = DatabaseManager(config_path)
        self.region     = region or cfg.get('aws', {}).get('region', 'us-east-1')
        self.hours      = hours
        self.period     = 300   # 5-minute granularity — matches our BUCKET_MINUTES
        self.cluster    = cfg.get('aws', {}).get('ecs_cluster', '')
        self.rds_id     = cfg.get('aws', {}).get('rds_cluster_id', '')
        self.alb_arn_suffix = cfg.get('aws', {}).get('alb_arn_suffix', '')

        # Lazy import so the file is importable without boto3 installed
        try:
            import boto3
            self.cw = boto3.client('cloudwatch', region_name=self.region)
            self.ecs = boto3.client('ecs', region_name=self.region)
            logger.info(f'CloudWatch client ready (region={self.region})')
        except ImportError:
            logger.error('boto3 not installed — run: pip install boto3')
            raise

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _time_window(self):
        end   = datetime.now(timezone.utc)
        start = end - timedelta(hours=self.hours)
        return start, end

    def _get_metric_stats(
        self,
        namespace: str,
        metric_name: str,
        dimensions: List[Dict],
        stat: str = 'Average',
    ) -> List[Dict]:
        """Fetch one metric series from CloudWatch."""
        start, end = self._time_window()
        try:
            resp = self.cw.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=self.period,
                Statistics=[stat],
            )
            return sorted(resp.get('Datapoints', []), key=lambda x: x['Timestamp'])
        except Exception as e:
            logger.warning(f'  CW error for {metric_name}: {e}')
            return []

    def _discover_ecs_services(self) -> List[str]:
        """Return list of ECS service names in the cluster."""
        if not self.cluster:
            logger.warning('aws.ecs_cluster not set in config — skipping ECS discovery')
            return []
        try:
            paginator = self.ecs.get_paginator('list_services')
            arns = []
            for page in paginator.paginate(cluster=self.cluster):
                arns.extend(page['serviceArns'])
            # ARN format: arn:aws:ecs:region:account:service/cluster/service-name
            return [a.split('/')[-1] for a in arns]
        except Exception as e:
            logger.error(f'ECS service discovery failed: {e}')
            return []

    # ── ECS metrics ──────────────────────────────────────────────────────────

    def ingest_ecs_service(self, service_name: str) -> int:
        """Pull ECS metrics for one service and write to SQLite.

        Returns number of rows written.
        """
        logger.info(f'  ECS service: {service_name}')
        inserted = 0

        for cw_name, internal_name in ECS_METRIC_MAP.items():
            dims = [
                {'Name': 'ClusterName', 'Value': self.cluster},
                {'Name': 'ServiceName', 'Value': service_name},
            ]
            stat = 'Sum' if cw_name == 'RunningTaskCount' else 'Average'
            points = self._get_metric_stats(
                'AWS/ECS', cw_name, dims, stat
            )

            for pt in points:
                ts  = pt['Timestamp'].replace(tzinfo=None).isoformat()
                val = pt.get(stat, 0.0)
                # FreeableMemory arrives in bytes — convert to MB
                if cw_name == 'FreeableMemory':
                    val = val / (1024 * 1024)
                try:
                    self.db.insert_metric(
                        timestamp=ts,
                        service_name=service_name,
                        metric_name=internal_name,
                        metric_value=round(float(val), 4),
                        metric_type=internal_name,
                        source='cloudwatch',
                    )
                    inserted += 1
                except Exception:
                    pass  # UNIQUE constraint — duplicate window, skip

            if points:
                logger.info(f'    {cw_name} ({internal_name}): {len(points)} points')

        return inserted

    # ── RDS metrics ──────────────────────────────────────────────────────────

    def ingest_rds(self, service_name: str = 'rds') -> int:
        """Pull RDS cluster metrics. service_name is the logical label in SQLite."""
        if not self.rds_id:
            logger.info('  aws.rds_cluster_id not set — skipping RDS')
            return 0

        logger.info(f'  RDS cluster: {self.rds_id}')
        inserted = 0

        for cw_name, internal_name in RDS_METRIC_MAP.items():
            dims = [{'Name': 'DBClusterIdentifier', 'Value': self.rds_id}]
            stat = 'Average' if cw_name != 'DatabaseConnections' else 'Maximum'
            points = self._get_metric_stats('AWS/RDS', cw_name, dims, stat)

            for pt in points:
                ts  = pt['Timestamp'].replace(tzinfo=None).isoformat()
                val = pt.get(stat, 0.0)
                if cw_name == 'FreeableMemory':
                    val = val / (1024 * 1024)
                try:
                    self.db.insert_metric(
                        timestamp=ts,
                        service_name=service_name,
                        metric_name=internal_name,
                        metric_value=round(float(val), 4),
                        metric_type=internal_name,
                        source='cloudwatch',
                    )
                    inserted += 1
                except Exception:
                    pass

            if points:
                logger.info(f'    {cw_name} ({internal_name}): {len(points)} points')

        return inserted

    # ── ALB metrics ──────────────────────────────────────────────────────────

    def ingest_alb(self, service_name: str, alb_arn_suffix: str) -> int:
        """Pull ALB metrics and attribute them to a service."""
        logger.info(f'  ALB for {service_name}: {alb_arn_suffix[:30]}...')
        inserted = 0

        for cw_name, internal_name in ALB_METRIC_MAP.items():
            dims = [{'Name': 'LoadBalancer', 'Value': alb_arn_suffix}]
            stat = 'Sum' if 'Count' in cw_name else 'Average'
            points = self._get_metric_stats(
                'AWS/ApplicationELB', cw_name, dims, stat
            )

            for pt in points:
                ts  = pt['Timestamp'].replace(tzinfo=None).isoformat()
                val = pt.get(stat, 0.0)
                # Convert TargetResponseTime seconds → milliseconds
                if cw_name == 'TargetResponseTime':
                    val = val * 1000
                try:
                    self.db.insert_metric(
                        timestamp=ts,
                        service_name=service_name,
                        metric_name=internal_name,
                        metric_value=round(float(val), 4),
                        metric_type=internal_name,
                        source='cloudwatch',
                    )
                    inserted += 1
                except Exception:
                    pass

            if points:
                logger.info(f'    {cw_name} ({internal_name}): {len(points)} points')

        return inserted

    # ── Main run ──────────────────────────────────────────────────────────────

    def run(self) -> int:
        """Pull all configured metrics. Returns total rows inserted."""
        total = 0
        logger.info(f'CloudWatch ingest — last {self.hours}h, region={self.region}')

        # ECS services
        ecs_services = self._discover_ecs_services()
        if ecs_services:
            logger.info(f'Found {len(ecs_services)} ECS services')
            for svc in ecs_services:
                total += self.ingest_ecs_service(svc)
        else:
            logger.warning('No ECS services found — check aws.ecs_cluster in config.yaml')

        # RDS
        total += self.ingest_rds()

        # ALB (if configured)
        if self.alb_arn_suffix:
            total += self.ingest_alb('alb', self.alb_arn_suffix)

        logger.info(f'\n=== CW INGEST SUMMARY ===')
        logger.info(f'Total metric rows inserted: {total}')
        return total


def main():
    parser = argparse.ArgumentParser(description='Ingest CloudWatch metrics into SQLite')
    parser.add_argument('--hours',  type=int, default=24,
                        help='How many hours back to pull (default 24)')
    parser.add_argument('--region', type=str, default=None,
                        help='AWS region (overrides config.yaml)')
    args = parser.parse_args()

    ingestor = CloudWatchIngestor(hours=args.hours, region=args.region)
    ingestor.run()
    return 0


if __name__ == '__main__':
    sys.exit(main())

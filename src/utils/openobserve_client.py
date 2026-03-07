"""OpenObserve client for log ingestion and querying."""
import requests
import logging
from typing import List, Dict, Any, Optional
import yaml
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class OpenObserveClient:
    """Client for OpenObserve REST API."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize OpenObserve client.
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        oo_config = config['openobserve']
        self.base_url = oo_config['url']
        self.username = oo_config['username']
        self.password = oo_config['password']
        self.organization = oo_config['organization']
        self.stream_name = oo_config['stream_name']
        self.batch_size = oo_config['batch_size']
        self.timeout = oo_config['timeout']
        
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def check_health(self) -> bool:
        """Check if OpenObserve is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            response = self.session.get(
                f"{self.base_url}/healthz",
                timeout=self.timeout
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def ingest_logs(self, logs: List[Dict[str, Any]]) -> bool:
        """Ingest logs to OpenObserve.
        
        Args:
            logs: List of log dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not logs:
            return True
        
        url = f"{self.base_url}/api/{self.organization}/{self.stream_name}/_json"
        
        # Process in batches
        total_ingested = 0
        for i in range(0, len(logs), self.batch_size):
            batch = logs[i:i + self.batch_size]
            
            try:
                response = self.session.post(
                    url,
                    json=batch,
                    timeout=self.timeout
                )
                
                if response.status_code in [200, 201]:
                    total_ingested += len(batch)
                    logger.debug(f"Ingested batch {i//self.batch_size + 1}: {len(batch)} logs")
                else:
                    logger.error(f"Ingestion failed: {response.status_code} - {response.text}")
                    return False
                
            except Exception as e:
                logger.error(f"Ingestion error: {e}")
                return False
            
            # Rate limiting
            time.sleep(0.1)
        
        logger.info(f"Successfully ingested {total_ingested} logs")
        return True
    
    def execute_sql(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        """Execute SQL query against logs.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of result rows or None if failed
        """
        url = f"{self.base_url}/api/{self.organization}/_search"
        
        payload = {
            "query": {
                "sql": sql,
                "start_time": 0,
                "end_time": int(time.time() * 1000000),  # microseconds
                "from": 0,
                "size": 10000
            }
        }
        
        try:
            response = self.session.post(
                url,
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('hits', [])
            else:
                logger.error(f"SQL query failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"SQL query error: {e}")
            return None
    
    def get_error_rate(self, service_name: str, window_minutes: int = 60) -> Optional[float]:
        """Calculate error rate for a service.
        
        Args:
            service_name: Name of service
            window_minutes: Time window in minutes
            
        Returns:
            Error rate as percentage or None if failed
        """
        sql = f"""
        SELECT 
            service_name,
            COUNT(*) as total_logs,
            SUM(CASE WHEN is_error = true THEN 1 ELSE 0 END) as error_count,
            (SUM(CASE WHEN is_error = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as error_rate
        FROM {self.stream_name}
        WHERE service_name = '{service_name}'
        AND timestamp_dt >= NOW() - INTERVAL '{window_minutes}' MINUTE
        GROUP BY service_name
        """
        
        results = self.execute_sql(sql)
        if results and len(results) > 0:
            return results[0].get('error_rate', 0.0)
        return None
    
    def get_latency_percentile(self, service_name: str, percentile: int = 95, 
                               window_minutes: int = 60) -> Optional[float]:
        """Calculate latency percentile for a service.
        
        Args:
            service_name: Name of service
            percentile: Percentile to calculate (50, 95, 99)
            window_minutes: Time window in minutes
            
        Returns:
            Latency in milliseconds or None if failed
        """
        sql = f"""
        SELECT 
            service_name,
            APPROX_PERCENTILE(latency_ms, {percentile/100.0}) as p{percentile}
        FROM {self.stream_name}
        WHERE service_name = '{service_name}'
        AND latency_ms IS NOT NULL
        AND timestamp_dt >= NOW() - INTERVAL '{window_minutes}' MINUTE
        GROUP BY service_name
        """
        
        results = self.execute_sql(sql)
        if results and len(results) > 0:
            return results[0].get(f'p{percentile}', 0.0)
        return None
    
    def discover_services(self) -> List[str]:
        """Discover all services from logs.
        
        Returns:
            List of unique service names
        """
        sql = f"""
        SELECT DISTINCT service_name
        FROM {self.stream_name}
        ORDER BY service_name
        """
        
        results = self.execute_sql(sql)
        if results:
            return [r['service_name'] for r in results]
        return []
    
    def get_log_count(self, service_name: Optional[str] = None, 
                     window_minutes: int = 60) -> int:
        """Get total log count.
        
        Args:
            service_name: Optional service name filter
            window_minutes: Time window in minutes
            
        Returns:
            Total log count
        """
        where_clause = f"WHERE service_name = '{service_name}'" if service_name else ""
        if where_clause:
            where_clause += f" AND timestamp_dt >= NOW() - INTERVAL '{window_minutes}' MINUTE"
        else:
            where_clause = f"WHERE timestamp_dt >= NOW() - INTERVAL '{window_minutes}' MINUTE"
        
        sql = f"""
        SELECT COUNT(*) as total
        FROM {self.stream_name}
        {where_clause}
        """
        
        results = self.execute_sql(sql)
        if results and len(results) > 0:
            return results[0].get('total', 0)
        return 0
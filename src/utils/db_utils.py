"""Database utility functions for SQLite operations."""
import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database operations."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize database manager.
        
        Args:
            config_path: Path to configuration file
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.db_path = config['database']['path']
        self.timeout = config['database']['connection_timeout']
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Create database and tables if they don't exist."""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Read schema
        schema_path = Path("config/schema.sql")
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            return
        
        with open(schema_path, 'r') as f:
            schema = f.read()
        
        # Execute schema
        with self.get_connection() as conn:
            conn.executescript(schema)
            conn.commit()
        
        logger.info(f"Database initialized at {self.db_path}")
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection.
        
        Returns:
            SQLite connection object
        """
        conn = sqlite3.connect(self.db_path, timeout=self.timeout)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        start_time = datetime.now()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                # Convert rows to dictionaries
                columns = [description[0] for description in cursor.description] if cursor.description else []
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                self._log_query('SELECT', query, execution_time, len(results))
                
                return results
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self._log_query('SELECT', query, execution_time, 0, str(e))
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_insert(self, query: str, params: tuple = ()) -> int:
        """Execute INSERT query.
        
        Args:
            query: SQL INSERT statement
            params: Query parameters
            
        Returns:
            Last inserted row ID
        """
        start_time = datetime.now()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                self._log_query('INSERT', query, execution_time, 1)
                
                return cursor.lastrowid
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self._log_query('INSERT', query, execution_time, 0, str(e))
            logger.error(f"Insert failed: {e}")
            raise
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute INSERT query with multiple parameter sets.
        
        Args:
            query: SQL INSERT statement
            params_list: List of parameter tuples
            
        Returns:
            Number of rows inserted
        """
        start_time = datetime.now()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, params_list)
                conn.commit()
                
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                self._log_query('INSERT_MANY', query, execution_time, len(params_list))
                
                return len(params_list)
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self._log_query('INSERT_MANY', query, execution_time, 0, str(e))
            logger.error(f"Batch insert failed: {e}")
            raise
    
    def _log_query(self, query_type: str, query_text: str, execution_time: float, 
                   result_count: int, error: Optional[str] = None):
        """Log query execution to query_log table.
        
        Args:
            query_type: Type of query (SELECT, INSERT, etc.)
            query_text: The SQL query
            execution_time: Execution time in milliseconds
            result_count: Number of affected/returned rows
            error: Error message if query failed
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO query_log 
                       (query_type, query_text, execution_time_ms, result_count, error, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (query_type, query_text, execution_time, result_count, error, datetime.now().isoformat())
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to log query: {e}")
    
    def insert_incident(self, timestamp: str, service_name: str, incident_type: str,
                       severity: str, description: str, root_cause: Optional[str] = None,
                       affected_metrics: Optional[str] = None, confidence_score: Optional[float] = None) -> int:
        """Insert new incident.
        
        Args:
            timestamp: Incident timestamp
            service_name: Name of affected service
            incident_type: Type of incident
            severity: Severity level
            description: Incident description
            root_cause: Identified root cause
            affected_metrics: JSON string of affected metrics
            confidence_score: Confidence score (0.0-1.0)
            
        Returns:
            Incident ID
        """
        query = """INSERT OR IGNORE INTO incidents 
                   (timestamp, service_name, incident_type, severity, description, 
                    root_cause, affected_metrics, confidence_score, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        
        return self.execute_insert(
            query,
            (timestamp, service_name, incident_type, severity, description,
             root_cause, affected_metrics, confidence_score, datetime.now().isoformat())
        )
    
    def insert_baseline(self, service_name: str, metric_name: str, mean: float, stddev: float,
                       p50: float, p95: float, p99: float, sample_count: int,
                       window_start: str, window_end: str) -> int:
        """Insert baseline statistics.
        
        Args:
            service_name: Name of service
            metric_name: Name of metric
            mean: Mean value
            stddev: Standard deviation
            p50: 50th percentile
            p95: 95th percentile
            p99: 99th percentile
            sample_count: Number of samples
            window_start: Start of time window
            window_end: End of time window
            
        Returns:
            Baseline ID
        """
        query = """INSERT OR REPLACE INTO baselines
                   (service_name, metric_name, mean, stddev, p50, p95, p99, 
                    sample_count, window_start, window_end, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        
        return self.execute_insert(
            query,
            (service_name, metric_name, mean, stddev, p50, p95, p99,
             sample_count, window_start, window_end, datetime.now().isoformat())
        )
    
    def insert_metric(self, timestamp: str, service_name: str, metric_name: str,
                     metric_value: float, metric_type: str, source: str) -> int:
        """Insert metric value.
        
        Args:
            timestamp: Metric timestamp
            service_name: Service name
            metric_name: Metric name
            metric_value: Metric value
            metric_type: Type of metric
            source: Data source
            
        Returns:
            Metric ID
        """
        query = """INSERT OR REPLACE INTO metrics
                   (timestamp, service_name, metric_name, metric_value, metric_type, source, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"""
        
        return self.execute_insert(
            query,
            (timestamp, service_name, metric_name, metric_value, metric_type, source, datetime.now().isoformat())
        )
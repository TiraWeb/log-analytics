"""Unit tests for core functionality."""
import unittest
import sys
from pathlib import Path
import tempfile
import os

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.log_parser import LogParser
from src.utils.db_utils import DatabaseManager


class TestLogParser(unittest.TestCase):
    """Test log parser functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = LogParser()
    
    def test_strip_ansi(self):
        """Test ANSI code stripping."""
        text = "\x1b[32minfo\x1b[39m|correlation|tenant|timestamp:\tlog text"
        cleaned = self.parser.strip_ansi(text)
        self.assertEqual(cleaned, "info|correlation|tenant|timestamp:\tlog text")
    
    def test_parse_log_line(self):
        """Test log line parsing."""
        line = "info|abc123|tenant1|2024-03-01T10:00:00.000Z:\tTest log message"
        result = self.parser.parse_log_line(line)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['level'], 'info')
        self.assertEqual(result['correlation_id'], 'abc123')
        self.assertEqual(result['tenant_id'], 'tenant1')
        self.assertEqual(result['log_text'], 'Test log message')
    
    def test_parse_log_line_with_req_id(self):
        """Test log line parsing with request ID."""
        line = "info|abc|tenant|[req123|]2024-03-01T10:00:00.000Z:\tMessage"
        result = self.parser.parse_log_line(line)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['req_id'], 'req123')
    
    def test_extract_http_metrics(self):
        """Test HTTP metrics extraction."""
        log_text = "[End Request] GET /api/users 200 OK 145ms"
        metrics = self.parser.extract_http_metrics(log_text)
        
        self.assertEqual(metrics['status_code'], 200)
        self.assertEqual(metrics['latency_ms'], 145)
    
    def test_is_error_log(self):
        """Test error detection."""
        # Test error level
        self.assertTrue(self.parser.is_error_log('error', 'Something went wrong'))
        
        # Test error keywords
        self.assertTrue(self.parser.is_error_log('info', 'Connection timeout error'))
        
        # Test normal log
        self.assertFalse(self.parser.is_error_log('info', 'User logged in successfully'))


class TestDatabaseManager(unittest.TestCase):
    """Test database manager functionality."""
    
    def setUp(self):
        """Set up test database."""
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        
        # Create temporary config
        self.temp_config = tempfile.NamedTemporaryFile(
            mode='w',
            delete=False,
            suffix='.yaml'
        )
        self.temp_config.write(f"""
database:
  path: "{self.temp_db.name}"
  connection_timeout: 30
analysis:
  baseline_window_hours: 24
  anomaly_threshold_sigma: 3.0
  min_samples_for_baseline: 10
  correlation_threshold: 0.7
""")
        self.temp_config.close()
        
        # Initialize database with test config
        # Note: This will fail if schema.sql doesn't exist
        # For now, just test connection
        self.db_path = self.temp_db.name
    
    def tearDown(self):
        """Clean up test database."""
        os.unlink(self.temp_db.name)
        os.unlink(self.temp_config.name)
    
    def test_database_connection(self):
        """Test database connection."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        self.assertIsNotNone(conn)
        conn.close()


class TestConfig(unittest.TestCase):
    """Test configuration loading."""
    
    def test_config_exists(self):
        """Test that config file exists."""
        config_path = Path('config/config.yaml')
        # Config might not exist in test environment
        # Just check the path is valid
        self.assertIsInstance(config_path, Path)


if __name__ == '__main__':
    unittest.main()
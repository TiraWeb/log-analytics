"""Log parser for microservice logs in pipe-delimited format."""
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class LogParser:
    """Parses microservice logs with ANSI color codes and pipe delimiters."""
    
    # ANSI color code pattern
    ANSI_PATTERN = re.compile(r'\x1b\[[0-9;]*m')
    
    # Log format: level|correlation_id|tenant_id|[req_id|]timestamp:\tlog_text
    LOG_PATTERN = re.compile(
        r'^([^|]+)\|([^|]+)\|([^|]+)\|(?:\[([^|]+)\|\])?([^:]+):\s+(.*)$'
    )
    
    # HTTP End Request pattern for latency extraction
    END_REQUEST_PATTERN = re.compile(r'\[End Request\].*?(\d+)ms')
    
    # HTTP status code pattern
    STATUS_CODE_PATTERN = re.compile(r'\b(\d{3})\b')
    
    # Error keywords
    ERROR_KEYWORDS = ['error', 'exception', 'failed', 'timeout', 'refused', 'denied']
    
    def __init__(self):
        """Initialize log parser."""
        self.stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'failed_lines': 0,
            'merged_stack_traces': 0
        }
    
    def strip_ansi(self, text: str) -> str:
        """Remove ANSI color codes from text.
        
        Args:
            text: Text with ANSI codes
            
        Returns:
            Clean text without ANSI codes
        """
        return self.ANSI_PATTERN.sub('', text)
    
    def parse_log_line(self, line: str) -> Optional[Dict[str, str]]:
        """Parse single log line.
        
        Args:
            line: Raw log line
            
        Returns:
            Dictionary with parsed fields or None if parsing fails
        """
        # Strip ANSI codes
        clean_line = self.strip_ansi(line.strip())
        
        if not clean_line:
            return None
        
        # Match log pattern
        match = self.LOG_PATTERN.match(clean_line)
        if not match:
            return None
        
        level, correlation_id, tenant_id, req_id, timestamp, log_text = match.groups()
        
        return {
            'level': level.strip(),
            'correlation_id': correlation_id.strip(),
            'tenant_id': tenant_id.strip(),
            'req_id': req_id.strip() if req_id else '',
            'timestamp': timestamp.strip(),
            'log_text': log_text.strip()
        }
    
    def extract_http_metrics(self, log_text: str) -> Dict[str, Optional[int]]:
        """Extract HTTP metrics from log text.
        
        Args:
            log_text: Log message text
            
        Returns:
            Dictionary with status_code and latency_ms
        """
        metrics = {'status_code': None, 'latency_ms': None}
        
        # Extract latency from [End Request] lines
        latency_match = self.END_REQUEST_PATTERN.search(log_text)
        if latency_match:
            metrics['latency_ms'] = int(latency_match.group(1))
        
        # Extract HTTP status code
        status_match = self.STATUS_CODE_PATTERN.search(log_text)
        if status_match:
            status_code = int(status_match.group(1))
            # Only consider valid HTTP status codes (100-599)
            if 100 <= status_code <= 599:
                metrics['status_code'] = status_code
        
        return metrics
    
    def is_error_log(self, level: str, log_text: str) -> bool:
        """Determine if log represents an error.
        
        Args:
            level: Log level
            log_text: Log message text
            
        Returns:
            True if log is an error
        """
        # Check log level
        if level.lower() in ['error', 'fatal', 'critical']:
            return True
        
        # Check for error keywords in text
        log_text_lower = log_text.lower()
        return any(keyword in log_text_lower for keyword in self.ERROR_KEYWORDS)
    
    def merge_stack_traces(self, logs: List[Dict]) -> List[Dict]:
        """Merge multi-line stack traces into single log entries.
        
        Stack traces are identified by consecutive logs with same timestamp
        where subsequent lines don't match the log pattern.
        
        Args:
            logs: List of parsed log dictionaries
            
        Returns:
            List of logs with merged stack traces
        """
        if not logs:
            return logs
        
        merged = []
        current_log = logs[0].copy()
        
        for i in range(1, len(logs)):
            log = logs[i]
            
            # If timestamp matches and it's a continuation (empty structured fields)
            if (log['timestamp'] == current_log['timestamp'] and 
                not log['level'] and not log['correlation_id']):
                
                # Append to current log text
                current_log['log_text'] += '\n' + log['log_text']
                self.stats['merged_stack_traces'] += 1
            else:
                # New log entry
                merged.append(current_log)
                current_log = log.copy()
        
        # Add last log
        merged.append(current_log)
        
        return merged
    
    def parse_csv_file(self, filepath: str, service_name: str) -> pd.DataFrame:
        """Parse CSV log file.
        
        Args:
            filepath: Path to CSV file
            service_name: Name of the service (extracted from filename)
            
        Returns:
            DataFrame with parsed and enriched logs
        """
        logger.info(f"Parsing {filepath} for service {service_name}")
        
        # Read CSV
        try:
            df = pd.read_csv(filepath, header=None, names=['raw_log'])
        except Exception as e:
            logger.error(f"Failed to read CSV {filepath}: {e}")
            return pd.DataFrame()
        
        self.stats['total_lines'] = len(df)
        
        # Parse each line
        parsed_logs = []
        for _, row in df.iterrows():
            parsed = self.parse_log_line(row['raw_log'])
            if parsed:
                parsed_logs.append(parsed)
                self.stats['parsed_lines'] += 1
            else:
                self.stats['failed_lines'] += 1
        
        if not parsed_logs:
            logger.warning(f"No logs successfully parsed from {filepath}")
            return pd.DataFrame()
        
        # Merge stack traces
        parsed_logs = self.merge_stack_traces(parsed_logs)
        
        # Convert to DataFrame
        df_parsed = pd.DataFrame(parsed_logs)
        
        # Add service name
        df_parsed['service_name'] = service_name
        
        # Extract HTTP metrics
        http_metrics = df_parsed['log_text'].apply(self.extract_http_metrics)
        df_parsed['status_code'] = http_metrics.apply(lambda x: x['status_code'])
        df_parsed['latency_ms'] = http_metrics.apply(lambda x: x['latency_ms'])
        
        # Detect errors
        df_parsed['is_error'] = df_parsed.apply(
            lambda row: self.is_error_log(row['level'], row['log_text']),
            axis=1
        )
        
        # Convert timestamp to datetime
        try:
            df_parsed['timestamp_dt'] = pd.to_datetime(df_parsed['timestamp'])
        except Exception as e:
            logger.warning(f"Failed to parse timestamps: {e}")
            df_parsed['timestamp_dt'] = pd.NaT
        
        logger.info(
            f"Parsed {self.stats['parsed_lines']}/{self.stats['total_lines']} lines, "
            f"merged {self.stats['merged_stack_traces']} stack traces, "
            f"found {df_parsed['is_error'].sum()} errors"
        )
        
        return df_parsed
    
    def get_stats(self) -> Dict[str, int]:
        """Get parsing statistics.
        
        Returns:
            Dictionary with parsing stats
        """
        return self.stats.copy()
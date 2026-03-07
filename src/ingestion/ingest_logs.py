"""ETL pipeline for ingesting CSV log files into OpenObserve."""
import sys
import logging
from pathlib import Path
import pandas as pd
from typing import List, Dict

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.log_parser import LogParser
from src.utils.openobserve_client import OpenObserveClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_service_name(filename: str) -> str:
    """Extract service name from filename.
    
    Examples:
        Internal-core-ms.csv -> Internal-core-ms
        External-core-ms-2.csv -> External-core-ms-2
        User-ms-3.csv -> User-ms-3
        Chargebee-EH-4.csv -> Chargebee-EH-4
    
    Args:
        filename: CSV filename
        
    Returns:
        Service name
    """
    return Path(filename).stem


def convert_to_openobserve_format(df: pd.DataFrame) -> List[Dict]:
    """Convert parsed DataFrame to OpenObserve format.
    
    Args:
        df: Parsed logs DataFrame
        
    Returns:
        List of log dictionaries ready for ingestion
    """
    logs = []
    
    for _, row in df.iterrows():
        log_entry = {
            '_timestamp': int(row['timestamp_dt'].timestamp() * 1000000) if pd.notna(row['timestamp_dt']) else 0,
            'service_name': row['service_name'],
            'level': row['level'],
            'correlation_id': row['correlation_id'],
            'tenant_id': row['tenant_id'],
            'req_id': row['req_id'],
            'timestamp': row['timestamp'],
            'log_text': row['log_text'],
            'is_error': bool(row['is_error']),
        }
        
        # Add optional fields if present
        if pd.notna(row['status_code']):
            log_entry['status_code'] = int(row['status_code'])
        
        if pd.notna(row['latency_ms']):
            log_entry['latency_ms'] = int(row['latency_ms'])
        
        logs.append(log_entry)
    
    return logs


def ingest_csv_file(filepath: Path, parser: LogParser, client: OpenObserveClient) -> bool:
    """Ingest single CSV file.
    
    Args:
        filepath: Path to CSV file
        parser: LogParser instance
        client: OpenObserveClient instance
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Processing {filepath.name}")
    
    # Extract service name
    service_name = extract_service_name(filepath.name)
    
    # Parse CSV
    df_parsed = parser.parse_csv_file(str(filepath), service_name)
    
    if df_parsed.empty:
        logger.warning(f"No logs parsed from {filepath.name}")
        return False
    
    # Convert to OpenObserve format
    logs = convert_to_openobserve_format(df_parsed)
    
    # Ingest to OpenObserve
    success = client.ingest_logs(logs)
    
    if success:
        logger.info(f"✓ Successfully ingested {len(logs)} logs from {filepath.name}")
    else:
        logger.error(f"✗ Failed to ingest logs from {filepath.name}")
    
    return success


def main():
    """Main ETL pipeline."""
    logger.info("Starting log ingestion pipeline")
    
    # Initialize clients
    parser = LogParser()
    client = OpenObserveClient()
    
    # Check OpenObserve health
    if not client.check_health():
        logger.error("OpenObserve is not healthy. Please start it with: docker-compose up -d")
        return 1
    
    logger.info("OpenObserve is healthy")
    
    # Find all CSV files in data/raw_logs/
    raw_logs_dir = Path("data/raw_logs")
    
    if not raw_logs_dir.exists():
        logger.error(f"Directory not found: {raw_logs_dir}")
        logger.info("Please create data/raw_logs/ and copy your CSV files there")
        return 1
    
    csv_files = list(raw_logs_dir.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {raw_logs_dir}")
        logger.info("Please copy your log CSV files to data/raw_logs/")
        return 1
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    success_count = 0
    for csv_file in csv_files:
        if ingest_csv_file(csv_file, parser, client):
            success_count += 1
    
    # Summary
    logger.info(f"\nIngestion complete: {success_count}/{len(csv_files)} files successful")
    
    # Show parsing stats
    stats = parser.get_stats()
    logger.info(f"Total lines: {stats['total_lines']}")
    logger.info(f"Parsed lines: {stats['parsed_lines']}")
    logger.info(f"Failed lines: {stats['failed_lines']}")
    logger.info(f"Merged stack traces: {stats['merged_stack_traces']}")
    
    return 0 if success_count == len(csv_files) else 1


if __name__ == "__main__":
    sys.exit(main())
"""Log parser for Velaris.io microservice logs.

CSV format produced by CloudWatch export:
    timestamp_ms , message
    1771896412325, <ANSI>level|corr_id|tenant|[req_id|]ISO_timestamp:\t<text>

Blank / stack-trace continuation rows have an empty or whitespace-only message.
"""
import re
import pandas as pd
from typing import Dict, List, Optional
from datetime import timezone
import logging

logger = logging.getLogger(__name__)


class LogParser:
    """Parses Velaris microservice logs with ANSI colour codes and pipe delimiters."""

    ANSI_PATTERN        = re.compile(r'\x1b\[[0-9;]*m')
    # Handles both 5-part (with req_id) and 4-part (without) pipe formats:
    #   level|corr_id|tenant_id|req_id|ISO_ts:\ttext
    #   level|corr_id|tenant_id|ISO_ts:\ttext
    LOG_PATTERN         = re.compile(
        r'^([^|]+)\|([^|]+)\|([^|]+)\|(?:([^|]+)\|)?([^:]+):\s+(.*)$',
        re.DOTALL
    )
    END_REQUEST_PATTERN = re.compile(r'\[End Request\].*?(\d+(?:\.\d+)?)\s*ms', re.I)
    HTTP_METHOD_PATH    = re.compile(r'\[End Request\]\s+(\w+)\s+(\S+)\s+(\d{3})')
    ERROR_KEYWORDS      = frozenset(['error', 'exception', 'failed', 'timeout',
                                     'refused', 'denied', 'fatal', 'critical',
                                     'unhandled', 'uncaught'])

    def __init__(self):
        self.stats = {
            'total_lines':       0,
            'parsed_lines':      0,
            'failed_lines':      0,
            'merged_stack_traces': 0,
        }

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def strip_ansi(self, text: str) -> str:
        return self.ANSI_PATTERN.sub('', text)

    def parse_log_line(self, line: str) -> Optional[Dict]:
        clean = self.strip_ansi(line.strip())
        if not clean:
            return None
        m = self.LOG_PATTERN.match(clean)
        if not m:
            return None
        level, corr_id, tenant_id, req_id, ts_raw, log_text = m.groups()
        return {
            'level':          level.strip().lower(),
            'correlation_id': corr_id.strip(),
            'tenant_id':      tenant_id.strip(),
            'req_id':         (req_id or '').strip(),
            'timestamp':      ts_raw.strip(),
            'log_text':       log_text.strip(),
        }

    def extract_http_metrics(self, log_text: str) -> Dict:
        metrics = {'status_code': None, 'latency_ms': None}
        lat_m = self.END_REQUEST_PATTERN.search(log_text)
        if lat_m:
            metrics['latency_ms'] = float(lat_m.group(1))
        ep_m = self.HTTP_METHOD_PATH.search(log_text)
        if ep_m:
            sc = int(ep_m.group(3))
            if 100 <= sc <= 599:
                metrics['status_code'] = sc
        return metrics

    def is_error_log(self, level: str, log_text: str) -> bool:
        if level in ('error', 'fatal', 'critical', 'warn', 'warning'):
            return True
        txt_lower = log_text.lower()
        return any(kw in txt_lower for kw in self.ERROR_KEYWORDS)

    # ------------------------------------------------------------------
    # CSV parsing
    # ------------------------------------------------------------------

    def parse_csv_file(self, filepath: str, service_name: str) -> pd.DataFrame:
        """Parse a CloudWatch-exported CSV and return an enriched DataFrame."""
        logger.info(f"Parsing {filepath}  (service={service_name})")

        try:
            # The CSV has a header row: timestamp,message
            raw = pd.read_csv(
                filepath,
                dtype=str,
                keep_default_na=False,
                on_bad_lines='skip',
            )
        except Exception as exc:
            logger.error(f"Cannot read CSV {filepath}: {exc}")
            return pd.DataFrame()

        # Normalise column names — handle files that have/lack a header
        raw.columns = [c.strip().lower() for c in raw.columns]
        if 'message' not in raw.columns:
            # No header — treat first col as timestamp, second as message
            raw.columns = ['timestamp_ms', 'message'] if len(raw.columns) == 2 else raw.columns
        if 'timestamp' in raw.columns and 'timestamp_ms' not in raw.columns:
            raw = raw.rename(columns={'timestamp': 'timestamp_ms'})

        self.stats['total_lines'] = len(raw)

        parsed_rows = []
        for _, row in raw.iterrows():
            msg = str(row.get('message', ''))
            parsed = self.parse_log_line(msg)
            if parsed:
                parsed['ts_ms'] = row.get('timestamp_ms', '')
                parsed_rows.append(parsed)
                self.stats['parsed_lines'] += 1
            else:
                self.stats['failed_lines'] += 1

        if not parsed_rows:
            logger.warning(f"Zero log lines parsed from {filepath}")
            return pd.DataFrame()

        df = pd.DataFrame(parsed_rows)
        df['service_name'] = service_name

        # Convert timestamp to datetime — prefer ISO string from log body;
        # fall back to the epoch-ms column from the CSV.
        def _to_dt(row):
            try:
                return pd.to_datetime(row['timestamp'], utc=True)
            except Exception:
                pass
            try:
                return pd.to_datetime(int(row['ts_ms']), unit='ms', utc=True)
            except Exception:
                return pd.NaT

        df['timestamp_dt'] = df.apply(_to_dt, axis=1)

        # HTTP metrics
        http = df['log_text'].apply(self.extract_http_metrics)
        df['status_code'] = http.apply(lambda x: x['status_code'])
        df['latency_ms']  = http.apply(lambda x: x['latency_ms'])

        # Error flag
        df['is_error'] = df.apply(
            lambda r: self.is_error_log(r['level'], r['log_text']), axis=1
        )

        error_count   = int(df['is_error'].sum())
        latency_count = int(df['latency_ms'].notna().sum())
        logger.info(
            f"  {self.stats['parsed_lines']}/{self.stats['total_lines']} parsed  "
            f"| {error_count} errors  | {latency_count} latency samples"
        )
        return df

    def get_stats(self) -> Dict:
        return self.stats.copy()

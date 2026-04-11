"""Log parser for Velaris.io microservice logs.

CSV format produced by CloudWatch export:
    timestamp_ms , message
    1771896412325, <ANSI>level|corr_id|tenant|[req_id|]ISO_timestamp:\t<text>

Blank / stack-trace continuation rows share the same timestamp as the parent
row and have no pipe-delimited prefix — they are now merged back onto the
parent log line instead of being counted as failed parses.
"""
import re
import csv
import io
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
    END_REQUEST_PATTERN = re.compile(r'\[End Request\].*?([\d.]+)\s*ms', re.I)
    HTTP_METHOD_PATH    = re.compile(r'\[End Request\]\s+(\w+)\s+(\S+)\s+(\d{3})')
    ERROR_KEYWORDS      = frozenset(['error', 'exception', 'failed', 'timeout',
                                     'refused', 'denied', 'fatal', 'critical',
                                     'unhandled', 'uncaught'])

    # A valid CloudWatch timestamp is exactly 13 decimal digits
    _TS_RE = re.compile(r'^\d{13}$')

    def __init__(self):
        self.stats = {
            'total_lines':         0,
            'parsed_lines':        0,
            'failed_lines':        0,
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
    # CSV parsing  (multiline-aware)
    # ------------------------------------------------------------------

    def parse_csv_file(self, filepath: str, service_name: str) -> pd.DataFrame:
        """Parse a CloudWatch-exported CSV and return an enriched DataFrame.

        Handles multiline / stack-trace rows: if a CSV row's timestamp column
        is NOT a 13-digit epoch-ms value it is treated as a continuation of
        the previous row's message and merged in rather than discarded.
        """
        logger.info(f"Parsing {filepath}  (service={service_name})")

        try:
            with open(filepath, newline='', encoding='utf-8', errors='replace') as fh:
                raw_text = fh.read()
        except Exception as exc:
            logger.error(f"Cannot read CSV {filepath}: {exc}")
            return pd.DataFrame()

        # ---- 1. Read raw rows via csv module (handles quoted newlines) ----
        reader = csv.reader(io.StringIO(raw_text))
        try:
            header = [c.strip().lower() for c in next(reader)]
        except StopIteration:
            logger.warning(f"{filepath} is empty")
            return pd.DataFrame()

        if 'timestamp' not in header and 'timestamp_ms' not in header:
            logger.warning(f"{filepath}: unexpected header {header}")
            return pd.DataFrame()

        ts_idx  = header.index('timestamp_ms') if 'timestamp_ms' in header else header.index('timestamp')
        msg_idx = header.index('message') if 'message' in header else 1

        # ---- 2. Merge continuation lines ----
        merged: list[tuple[str, str]] = []   # (timestamp_str, full_message)
        pending_ts  = None
        pending_msg = None

        def _flush():
            nonlocal pending_ts, pending_msg
            if pending_ts is not None and pending_msg is not None:
                merged.append((pending_ts, pending_msg))
            pending_ts = pending_msg = None

        for row in reader:
            if not row:
                continue
            raw_ts  = row[ts_idx].strip()  if len(row) > ts_idx  else ''
            raw_msg = row[msg_idx]         if len(row) > msg_idx else ''

            if self._TS_RE.match(raw_ts):
                # New log entry — flush previous
                _flush()
                pending_ts  = raw_ts
                pending_msg = raw_msg
            else:
                # Continuation / stack trace line — append to current
                if pending_msg is not None:
                    pending_msg += '\n' + raw_msg
                    self.stats['merged_stack_traces'] += 1

        _flush()

        self.stats['total_lines'] = len(merged)

        # ---- 3. Parse each merged row ----
        parsed_rows = []
        for ts_str, msg in merged:
            parsed = self.parse_log_line(msg)
            if parsed:
                parsed['ts_ms'] = ts_str
                parsed_rows.append(parsed)
                self.stats['parsed_lines'] += 1
            else:
                self.stats['failed_lines'] += 1

        if not parsed_rows:
            logger.warning(f"Zero log lines parsed from {filepath}")
            return pd.DataFrame()

        df = pd.DataFrame(parsed_rows)
        df['service_name'] = service_name

        # ---- 4. Timestamps ----
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

        # ---- 5. HTTP metrics ----
        http = df['log_text'].apply(self.extract_http_metrics)
        df['status_code'] = http.apply(lambda x: x['status_code'])
        df['latency_ms']  = http.apply(lambda x: x['latency_ms'])

        # ---- 6. Error flag ----
        df['is_error'] = df.apply(
            lambda r: self.is_error_log(r['level'], r['log_text']), axis=1
        )

        error_count   = int(df['is_error'].sum())
        latency_count = int(df['latency_ms'].notna().sum())
        logger.info(
            f"  {self.stats['parsed_lines']}/{self.stats['total_lines']} parsed  "
            f"| {error_count} errors  | {latency_count} latency samples  "
            f"| {self.stats['merged_stack_traces']} continuation lines merged"
        )
        return df

    def get_stats(self) -> Dict:
        return self.stats.copy()

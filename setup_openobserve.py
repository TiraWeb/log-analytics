#!/usr/bin/env python3
"""One-shot OpenObserve setup + log push.

What this script does
---------------------
1. Starts OpenObserve via docker-compose (or verifies it's already running).
2. Polls the health endpoint until it's ready (up to 60 s).
3. Creates / confirms the `microservice_logs` stream with the correct
   field-type mappings so numeric fields aren't stored as strings.
4. Reads every CSV in data/raw_logs/, parses it with LogParser, and
   bulk-pushes every log line as a structured JSON document to OpenObserve
   via the /_json ingest endpoint.
5. Prints a summary table and the UI URL when done.

Usage
-----
    python setup_openobserve.py                          # use defaults
    python setup_openobserve.py --email me@co.com --password s3cret
    python setup_openobserve.py --no-docker              # OO already running
    python setup_openobserve.py --logs-dir /my/logs      # custom log dir
    python setup_openobserve.py --dry-run                # parse only, no push

Requirements
------------
    pip install requests pyyaml pandas
    docker  (only needed unless --no-docker is passed)
"""
import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml

# Bring local modules onto path
sys.path.append(str(Path(__file__).parent))
from src.utils.log_parser import LogParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("setup_oo")

# ───────────────────────────────────────────────────────────────────────────────
# Constants
# ───────────────────────────────────────────────────────────────────────────────

DEFAULT_URL       = "http://localhost:5080"
DEFAULT_EMAIL     = "admin@example.com"
DEFAULT_PASSWORD  = "admin123"
DEFAULT_ORG      = "default"
STREAM_NAME       = "microservice_logs"
BATCH_SIZE        = 2_000   # documents per ingest request
HEALTH_RETRIES    = 30      # x 2 s = 60 s max wait
HEALTH_INTERVAL   = 2       # seconds between health polls

# Field mappings tell OpenObserve how to type each field.
# Without this, numeric-looking strings may be stored as keywords.
STREAM_MAPPINGS = {
    "mappings": {
        "fields": {
            "_timestamp":     {"data_type": "Long"},     # epoch-μs
            "latency_ms":     {"data_type": "Float"},
            "status_code":    {"data_type": "Long"},
            "is_error":       {"data_type": "Boolean"},
            "service_name":   {"data_type": "Keyword"},
            "log_level":      {"data_type": "Keyword"},
            "tenant_id":      {"data_type": "Keyword"},
            "correlation_id": {"data_type": "Keyword"},
        }
    }
}


# ───────────────────────────────────────────────────────────────────────────────
# Docker helpers
# ───────────────────────────────────────────────────────────────────────────────

def _run(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    log.info(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def is_container_running(name: str = "openobserve") -> bool:
    result = _run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def start_openobserve(email: str, password: str) -> None:
    """Write credentials into the compose file env vars, then docker compose up."""
    compose_file = Path("docker-compose.yml")
    if not compose_file.exists():
        log.error("docker-compose.yml not found in current directory.")
        sys.exit(1)

    # Patch credentials into a temporary override file so we don't
    # modify the committed docker-compose.yml.
    override = {
        "services": {
            "openobserve": {
                "environment": [
                    f"ZO_ROOT_USER_EMAIL={email}",
                    f"ZO_ROOT_USER_PASSWORD={password}",
                    "ZO_DATA_DIR=/data",
                ]
            }
        }
    }
    override_path = Path("docker-compose.override.yml")
    with open(override_path, "w") as f:
        yaml.dump(override, f, default_flow_style=False)
    log.info(f"Wrote {override_path} with supplied credentials")

    _run(["docker", "compose", "up", "-d", "--pull", "always"])


# ───────────────────────────────────────────────────────────────────────────────
# OpenObserve API helpers
# ───────────────────────────────────────────────────────────────────────────────

class OOClient:
    """Minimal OpenObserve HTTP client for setup operations."""

    def __init__(self, base_url: str, email: str, password: str, org: str):
        self.base = base_url.rstrip("/")
        self.org  = org
        self.session = requests.Session()
        self.session.auth = (email, password)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })

    def wait_until_ready(self, retries: int = HEALTH_RETRIES,
                         interval: int = HEALTH_INTERVAL) -> bool:
        log.info(f"Waiting for OpenObserve at {self.base}/healthz …")
        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(f"{self.base}/healthz", timeout=5)
                if r.status_code == 200:
                    log.info(f"  ✔ OpenObserve is healthy (attempt {attempt})")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            log.info(f"  [{attempt}/{retries}] not ready yet — waiting {interval}s")
            time.sleep(interval)
        return False

    def configure_stream(self, stream: str, mappings: dict) -> bool:
        """PUT field mappings on the stream (idempotent)."""
        url = f"{self.base}/api/{self.org}/{stream}/_settings"
        r = self.session.put(url, json=mappings, timeout=15)
        if r.status_code in (200, 201, 204):
            log.info(f"  ✔ Stream '{stream}' configured")
            return True
        # 400 can mean "stream doesn't exist yet" — first ingest creates it,
        # so this is not fatal.
        log.warning(f"  Stream config returned {r.status_code}: {r.text[:200]}")
        return False

    def ingest_batch(self, stream: str, docs: List[Dict]) -> bool:
        """POST a batch of documents to /<org>/<stream>/_json."""
        url = f"{self.base}/api/{self.org}/{stream}/_json"
        r = self.session.post(url, json=docs, timeout=60)
        if r.status_code in (200, 201):
            return True
        log.error(f"  Ingest failed {r.status_code}: {r.text[:300]}")
        return False


# ───────────────────────────────────────────────────────────────────────────────
# Log → document conversion
# ───────────────────────────────────────────────────────────────────────────────

def df_to_documents(df, service_name: str) -> List[Dict]:
    """Convert a parsed log DataFrame into OpenObserve JSON documents.

    OpenObserve requires a `_timestamp` field in epoch-microseconds.
    All other fields are optional but we include as many as we have.
    """
    docs = []
    for _, row in df.iterrows():
        ts = row.get("timestamp_dt")
        if ts is None or (hasattr(ts, "isnull") and ts.isnull()):
            continue
        # epoch-microseconds
        try:
            epoch_us = int(ts.timestamp() * 1_000_000)
        except Exception:
            continue

        doc: Dict = {
            "_timestamp":     epoch_us,
            "service_name":   service_name,
            "log_level":      str(row.get("level", "")).upper(),
            "message":        str(row.get("log_text", "")),
            "correlation_id": str(row.get("correlation_id", "")),
            "tenant_id":      str(row.get("tenant_id", "")),
            "is_error":       bool(row.get("is_error", False)),
        }

        lat = row.get("latency_ms")
        if lat is not None and str(lat) not in ("", "nan", "None"):
            try:
                doc["latency_ms"] = float(lat)
            except (ValueError, TypeError):
                pass

        sc = row.get("status_code")
        if sc is not None and str(sc) not in ("", "nan", "None"):
            try:
                doc["status_code"] = int(float(sc))
            except (ValueError, TypeError):
                pass

        req_id = row.get("req_id", "")
        if req_id:
            doc["req_id"] = str(req_id)

        docs.append(doc)
    return docs


# ───────────────────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Set up OpenObserve and push Velaris log CSVs"
    )
    p.add_argument("--email",    default=DEFAULT_EMAIL,
                   help=f"OO root user email  (default: {DEFAULT_EMAIL})")
    p.add_argument("--password", default=DEFAULT_PASSWORD,
                   help=f"OO root user password  (default: {DEFAULT_PASSWORD})")
    p.add_argument("--url",      default=DEFAULT_URL,
                   help=f"OpenObserve base URL  (default: {DEFAULT_URL})")
    p.add_argument("--org",      default=DEFAULT_ORG,
                   help=f"OpenObserve organisation  (default: {DEFAULT_ORG})")
    p.add_argument("--logs-dir", default="data/raw_logs",
                   help="Directory containing CSV log files  (default: data/raw_logs)")
    p.add_argument("--no-docker", action="store_true",
                   help="Skip docker-compose — assume OO is already running")
    p.add_argument("--dry-run",  action="store_true",
                   help="Parse CSVs and print stats, but do NOT push to OO")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                   help=f"Documents per ingest request  (default: {BATCH_SIZE})")
    return p.parse_args()


def main():
    args = parse_args()

    print()
    print("=" * 60)
    print("  OpenObserve Setup + Log Push")
    print("=" * 60)
    print(f"  URL      : {args.url}")
    print(f"  Email    : {args.email}")
    print(f"  Org      : {args.org}")
    print(f"  Logs dir : {args.logs_dir}")
    print(f"  Dry run  : {args.dry_run}")
    print("=" * 60)
    print()

    # ── Step 1: Docker ─────────────────────────────────────────────────────
    if not args.no_docker:
        if is_container_running("openobserve"):
            log.info("openobserve container is already running — skipping docker compose up")
        else:
            log.info("Starting OpenObserve via docker compose …")
            start_openobserve(args.email, args.password)
    else:
        log.info("--no-docker: assuming OpenObserve is already running")

    # ── Step 2: Health check ───────────────────────────────────────────────────
    oo = OOClient(args.url, args.email, args.password, args.org)

    if not args.dry_run:
        if not oo.wait_until_ready():
            log.error(
                "OpenObserve did not become healthy in time.\n"
                "  • Check `docker logs openobserve`\n"
                "  • Make sure port 5080 is not blocked"
            )
            sys.exit(1)

        # ── Step 3: Configure stream mappings ────────────────────────────────
        log.info(f"Configuring stream '{STREAM_NAME}' …")
        oo.configure_stream(STREAM_NAME, STREAM_MAPPINGS)

    # ── Step 4: Parse CSVs ────────────────────────────────────────────────────
    logs_dir = Path(args.logs_dir)
    if not logs_dir.exists():
        log.error(f"Logs directory not found: {logs_dir}")
        log.error("  Create it and add your CSV files, then re-run.")
        sys.exit(1)

    csv_files = sorted(logs_dir.glob("*.csv"))
    if not csv_files:
        log.error(f"No CSV files found in {logs_dir}")
        sys.exit(1)

    log.info(f"Found {len(csv_files)} CSV file(s) in {logs_dir}")

    parser       = LogParser()
    grand_total  = 0
    grand_pushed = 0
    summary_rows = []

    for csv_path in csv_files:
        service_name = csv_path.stem
        log.info(f"\n▶ {csv_path.name}  →  service='{service_name}'")

        df = parser.parse_csv_file(str(csv_path), service_name)
        if df.empty:
            log.warning(f"  No rows parsed — skipping")
            summary_rows.append((service_name, 0, 0, "SKIPPED"))
            continue

        docs = df_to_documents(df, service_name)
        grand_total += len(docs)
        log.info(f"  Parsed {len(df)} rows  →  {len(docs)} valid documents")

        if args.dry_run:
            log.info(f"  [dry-run] would push {len(docs)} docs")
            summary_rows.append((service_name, len(df), len(docs), "DRY-RUN"))
            continue

        # Push in batches
        pushed = 0
        failed_batches = 0
        for i in range(0, len(docs), args.batch_size):
            batch = docs[i : i + args.batch_size]
            ok    = oo.ingest_batch(STREAM_NAME, batch)
            if ok:
                pushed         += len(batch)
                grand_pushed   += len(batch)
            else:
                failed_batches += 1

            batch_num = i // args.batch_size + 1
            total_batches = (len(docs) + args.batch_size - 1) // args.batch_size
            log.info(
                f"  Batch {batch_num}/{total_batches}  "
                f"+{len(batch)} docs  →  {'OK' if ok else 'FAIL'}"
            )

        status = "OK" if failed_batches == 0 else f"{failed_batches} batches FAILED"
        summary_rows.append((service_name, len(df), pushed, status))
        log.info(f"  ✔ {pushed}/{len(docs)} documents pushed")

    # ── Step 5: Summary ────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  {'Service':<30}  {'Parsed':>8}  {'Pushed':>8}  Status")
    print(f"  {'-'*30}  {'-'*8}  {'-'*8}  ------")
    for svc, parsed, pushed, status in summary_rows:
        print(f"  {svc:<30}  {parsed:>8,}  {pushed:>8,}  {status}")
    print(f"  {'TOTAL':<30}  {grand_total:>8,}  {grand_pushed:>8,}")
    print("=" * 60)

    if not args.dry_run:
        print()
        print(f"  ✔ Done! Open OpenObserve UI at:")
        print(f"    {args.url}/web/logs?stream={STREAM_NAME}")
        print()
        print("  Next step — pull metrics into SQLite:")
        print("    python src/ingestion/ingest_logs.py --source opensearch")
        print()


if __name__ == "__main__":
    main()

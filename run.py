#!/usr/bin/env python3
"""run.py — start the entire log-analytics stack with one command.

    python run.py                  # normal start
    python run.py --force-recreate # nuke OO container+volume and start fresh

What it does (in order)
-----------------------
1.  pip install -r requirements.txt
2.  Start OpenObserve via docker compose
3.  Wait until OpenObserve /healthz is green
4.  Configure the microservice_logs stream
5.  Parse every CSV in data/raw_logs/ and push to OpenObserve
6.  Wipe stale SQLite data, aggregate real metrics from OpenObserve
7.  Run evaluation (generates data/evaluation_results.json)
8.  Open http://localhost:8050 in the browser
9.  Start the Flask dashboard server on port 8050

Credentials
-----------
Priority order:
  1. OO_EMAIL / OO_PASSWORD environment variables
  2. Running container env  (docker inspect)
  3. docker-compose.override.yml
  4. OO factory defaults    (root@example.com / Complexpass#123)

If the container was previously started with DIFFERENT creds and you keep
getting 401s, run:

    python run.py --force-recreate

This stops the container, removes it and its named volume, then recreates
everything fresh with the creds written to docker-compose.override.yml.
"""
import os
import re
import subprocess
import sys
import time
import threading
import webbrowser
from pathlib import Path

# ── factory defaults (OO ships with these) ───────────────────────────────────
OO_FACTORY_EMAIL    = "root@example.com"
OO_FACTORY_PASSWORD = "Complexpass#123"

# ── tunables ─────────────────────────────────────────────────────────────────
OO_URL      = os.environ.get("OO_URL",    "http://localhost:5080")
OO_ORG      = os.environ.get("OO_ORG",   "default")
STREAM      = os.environ.get("OO_STREAM", "microservice_logs")
DASH_PORT   = int(os.environ.get("DASH_PORT", "8050"))
BATCH_SIZE  = 2_000
HEALTH_RETRIES  = 30
HEALTH_INTERVAL = 2

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# ── pretty printer ───────────────────────────────────────────────────────────
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def step(n, msg):  print(f"\n{BOLD}{CYAN}[{n}]{RESET} {msg}")
def ok(msg):       print(f"  {GREEN}\u2714{RESET}  {msg}")
def warn(msg):     print(f"  {YELLOW}\u26a0{RESET}  {msg}")
def fail(msg):     print(f"  {RED}\u2718{RESET}  {msg}"); sys.exit(1)


# ── Credential detection ─────────────────────────────────────────────────────
def detect_oo_credentials() -> tuple[str, str]:
    """Detect OO root credentials in priority order."""

    # 1. Explicit env vars always win
    if os.environ.get("OO_EMAIL") and os.environ.get("OO_PASSWORD"):
        return os.environ["OO_EMAIL"], os.environ["OO_PASSWORD"]

    # 2. Read from the RUNNING container via docker inspect
    try:
        r = subprocess.run(
            ["docker", "inspect", "--format",
             "{{range .Config.Env}}{{println .}}{{end}}",
             "openobserve"],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0:
            email = password = None
            for line in r.stdout.splitlines():
                if line.startswith("ZO_ROOT_USER_EMAIL="):
                    email = line.split("=", 1)[1].strip()
                elif line.startswith("ZO_ROOT_USER_PASSWORD="):
                    password = line.split("=", 1)[1].strip()
            if email and password:
                return email, password
    except Exception:
        pass

    # 3. Read from docker-compose.override.yml if it exists
    override_path = ROOT / "docker-compose.override.yml"
    if override_path.exists():
        content = override_path.read_text()
        em = re.search(r'ZO_ROOT_USER_EMAIL=(.+)', content)
        pw = re.search(r'ZO_ROOT_USER_PASSWORD=(.+)', content)
        if em and pw:
            return em.group(1).strip(), pw.group(1).strip()

    # 4. Fall back to OO factory defaults
    return OO_FACTORY_EMAIL, OO_FACTORY_PASSWORD


# ── Optional: nuke the container so fresh creds take effect ──────────────────
def force_recreate_oo(email: str, password: str):
    """Stop + remove the OO container and its named volume, then recreate."""
    print(f"  {YELLOW}--force-recreate{RESET}: stopping and removing openobserve...")

    # Find the named volume used by the compose service
    vol_result = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .Mounts}}{{.Name}} {{end}}", "openobserve"],
        capture_output=True, text=True
    )
    volumes = [v.strip() for v in (vol_result.stdout or "").split() if v.strip()]

    subprocess.run(["docker", "stop", "openobserve"],  capture_output=True)
    subprocess.run(["docker", "rm",   "openobserve"],  capture_output=True)

    for vol in volumes:
        r = subprocess.run(["docker", "volume", "rm", vol], capture_output=True, text=True)
        if r.returncode == 0:
            ok(f"Removed volume: {vol}")
        else:
            warn(f"Could not remove volume {vol}: {r.stderr.strip()[:100]}")

    # Write override with correct creds
    override = (
        "services:\n"
        "  openobserve:\n"
        "    environment:\n"
        f"      - ZO_ROOT_USER_EMAIL={email}\n"
        f"      - ZO_ROOT_USER_PASSWORD={password}\n"
        "      - ZO_DATA_DIR=/data\n"
    )
    (ROOT / "docker-compose.override.yml").write_text(override)

    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--pull", "always"],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        fail(f"docker compose up failed:\n{result.stderr[:600]}")
    ok("OpenObserve recreated with fresh credentials")


# ── Step 1 — install deps ────────────────────────────────────────────────────
def install_deps():
    step(1, "Installing Python dependencies ...")
    req = ROOT / "requirements.txt"
    if not req.exists():
        warn("requirements.txt not found — skipping")
        return
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        warn(f"pip had warnings:\n{result.stderr[:400]}")
    ok("Dependencies ready")


# ── Step 2 — docker compose ──────────────────────────────────────────────────
def start_openobserve(email: str, password: str, force: bool = False):
    step(2, "Starting OpenObserve via Docker ...")

    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        fail("Docker is not running. Start Docker Desktop and re-run.")

    if force:
        force_recreate_oo(email, password)
        return

    # Check if already running
    running = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", "openobserve"],
        capture_output=True, text=True
    )
    if running.returncode == 0 and running.stdout.strip() == "true":
        ok("openobserve container already running")
        return

    # Write override only on fresh start
    override = (
        "services:\n"
        "  openobserve:\n"
        "    environment:\n"
        f"      - ZO_ROOT_USER_EMAIL={email}\n"
        f"      - ZO_ROOT_USER_PASSWORD={password}\n"
        "      - ZO_DATA_DIR=/data\n"
    )
    (ROOT / "docker-compose.override.yml").write_text(override)

    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--pull", "always"],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        fail(f"docker compose up failed:\n{result.stderr[:600]}")
    ok("OpenObserve container started")


# ── Step 3 — health check ─────────────────────────────────────────────────────
def wait_for_oo():
    step(3, f"Waiting for OpenObserve at {OO_URL} ...")
    import requests
    for i in range(1, HEALTH_RETRIES + 1):
        try:
            r = requests.get(f"{OO_URL}/healthz", timeout=4)
            if r.status_code == 200:
                ok(f"OpenObserve is healthy  (attempt {i})")
                return
        except Exception:
            pass
        print(f"  [{i}/{HEALTH_RETRIES}] not ready — retrying in {HEALTH_INTERVAL}s", end="\r")
        time.sleep(HEALTH_INTERVAL)
    fail("OpenObserve never became healthy. Check: docker logs openobserve")


# ── Step 3b — verify auth works, warn clearly if not ─────────────────────────
def verify_auth(email: str, password: str) -> bool:
    """Quick auth test — GET /api/{org}/streams. Returns True if 200."""
    import requests
    try:
        r = requests.get(
            f"{OO_URL}/api/{OO_ORG}/streams",
            auth=(email, password), timeout=8
        )
        if r.status_code == 200:
            ok(f"Auth verified  ({email})")
            return True
        print(f"  {RED}Auth returned {r.status_code}{RESET} for {email}")
        if r.status_code == 401:
            print(f"\n  {YELLOW}TIP:{RESET} Your container was initialised with different")
            print(f"  credentials. Run:  {BOLD}python run.py --force-recreate{RESET}")
            print(f"  to wipe and restart with the creds in this script.\n")
        return False
    except Exception as e:
        warn(f"Auth check failed: {e}")
        return False


# ── Step 4 — stream config ────────────────────────────────────────────────────
def configure_stream(email: str, password: str):
    step(4, f"Configuring stream '{STREAM}' ...")
    import requests
    mappings = {
        "mappings": {
            "fields": {
                "_timestamp":     {"data_type": "Long"},
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
    url = f"{OO_URL}/api/{OO_ORG}/{STREAM}/_settings"
    try:
        r = requests.put(url, json=mappings, auth=(email, password), timeout=15)
        if r.status_code in (200, 201, 204):
            ok("Stream configured")
        else:
            warn(f"Stream config returned {r.status_code} (non-fatal)")
    except Exception as e:
        warn(f"Stream config request failed: {e} (non-fatal)")


# ── Step 5 — push CSV logs ────────────────────────────────────────────────────
def push_logs(email: str, password: str):
    step(5, "Parsing CSVs and pushing logs to OpenObserve ...")
    import requests
    from src.utils.log_parser import LogParser

    logs_dir = ROOT / "data" / "raw_logs"
    if not logs_dir.exists():
        warn(f"{logs_dir} not found — skipping log push")
        return

    csv_files = sorted(logs_dir.glob("*.csv"))
    if not csv_files:
        warn("No CSV files found in data/raw_logs/ — skipping log push")
        return

    print(f"  Found {len(csv_files)} CSV file(s)")

    session = requests.Session()
    session.auth = (email, password)
    session.headers.update({"Content-Type": "application/json"})

    parser = LogParser()
    total_pushed = 0

    for csv_path in csv_files:
        service = csv_path.stem
        # Strip trailing version suffixes: User-ms-4 -> User-ms
        clean_service = re.sub(r'-\d+$', '', service)
        df = parser.parse_csv_file(str(csv_path), clean_service)
        if df.empty:
            warn(f"  {csv_path.name}: no rows parsed")
            continue

        docs = []
        for _, row in df.iterrows():
            ts = row.get("timestamp_dt")
            if ts is None or (hasattr(ts, "isnull") and ts.isnull()):
                continue
            try:
                epoch_us = int(ts.timestamp() * 1_000_000)
            except Exception:
                continue

            doc = {
                "_timestamp":     epoch_us,
                "service_name":   clean_service,
                "log_level":      str(row.get("level", "")).upper(),
                "message":        str(row.get("log_text", "")),
                "correlation_id": str(row.get("correlation_id", "")),
                "tenant_id":      str(row.get("tenant_id", "")),
                "is_error":       bool(row.get("is_error", False)),
            }
            lat = row.get("latency_ms")
            if lat is not None and str(lat) not in ("", "nan", "None"):
                try: doc["latency_ms"] = float(lat)
                except: pass
            sc = row.get("status_code")
            if sc is not None and str(sc) not in ("", "nan", "None"):
                try: doc["status_code"] = int(float(sc))
                except: pass
            docs.append(doc)

        if not docs:
            warn(f"  {clean_service}: 0 valid docs after conversion")
            continue

        pushed = 0
        url = f"{OO_URL}/api/{OO_ORG}/{STREAM}/_json"
        for i in range(0, len(docs), BATCH_SIZE):
            batch = docs[i:i + BATCH_SIZE]
            try:
                r = session.post(url, json=batch, timeout=60)
                if r.status_code in (200, 201):
                    pushed += len(batch)
                else:
                    warn(f"  Batch failed {r.status_code}: {r.text[:200]}")
            except Exception as e:
                warn(f"  Batch error: {e}")

        ok(f"{clean_service}: {pushed:,} / {len(docs):,} docs pushed")
        total_pushed += pushed

    ok(f"Total pushed to OpenObserve: {total_pushed:,} documents")


# ── Step 6 — SQLite metric ingestion ─────────────────────────────────────────
def ingest_metrics():
    step(6, "Wiping stale SQLite data and aggregating real metrics ...")

    db_path = ROOT / "data" / "analytics.db"
    if db_path.exists():
        db_path.unlink()
        ok("Cleared old analytics.db")

    result = subprocess.run(
        [sys.executable, "src/ingestion/ingest_logs.py", "--source", "opensearch"],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        warn(f"Metric ingestion had errors (non-fatal):\n{result.stderr[:400]}")
    else:
        ok("Real metrics written to SQLite")


# ── Step 7 — run evaluation ───────────────────────────────────────────────────
def run_evaluation():
    step(7, "Running evaluation (precision / recall / F1) ...")

    out_path = ROOT / "data" / "evaluation_results.json"
    result = subprocess.run(
        [sys.executable, "src/analysis/evaluate.py"],
        cwd=ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        warn(f"Evaluation failed (non-fatal):\n{result.stderr[:400]}")
        # Write a placeholder so /api/evaluate never 404s
        import json
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({
            "generated_at": "N/A",
            "status": "no_incidents",
            "message": "No incidents detected yet — run more log data through the pipeline.",
            "precision": 0, "recall": 0, "f1": 0,
            "tp": 0, "fp": 0, "fn": 0,
            "ground_truth_count": 0, "detected_count": 0,
        }, indent=2))
        warn("Wrote placeholder evaluation_results.json")
    else:
        ok("Evaluation complete — data/evaluation_results.json written")


# ── Step 8 + 9 — open browser + start Flask dashboard ────────────────────────
def launch_dashboard():
    step(8, f"Launching dashboard at http://localhost:{DASH_PORT} ...")

    def _open():
        time.sleep(2)
        webbrowser.open(f"http://localhost:{DASH_PORT}")
    threading.Thread(target=_open, daemon=True).start()

    print(f"\n{BOLD}{GREEN}  \u2714 Everything is running!{RESET}")
    print(f"  Dashboard   \u2192  http://localhost:{DASH_PORT}")
    print(f"  OpenObserve \u2192  {OO_URL}/web")
    print(f"  Press Ctrl-C to stop.\n")

    step(9, "Starting Flask server (Ctrl-C to quit) ...")
    os.environ["FLASK_PORT"] = str(DASH_PORT)
    try:
        from src.dashboard.server import app
        app.run(host="0.0.0.0", port=DASH_PORT, debug=False)
    except ImportError:
        subprocess.run(
            [sys.executable, "src/dashboard/server.py"],
            cwd=ROOT,
            env={**os.environ, "FLASK_PORT": str(DASH_PORT)},
        )


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--force-recreate", action="store_true",
        help="Stop+remove the openobserve container and volume, then recreate fresh."
    )
    args = ap.parse_args()

    print(f"\n{BOLD}{'='*54}{RESET}")
    print(f"{BOLD}   Log Analytics \u2014 Full Stack Launcher{RESET}")
    print(f"{BOLD}{'='*54}{RESET}")
    if args.force_recreate:
        print(f"  {YELLOW}Mode: --force-recreate (container will be wiped){RESET}")

    install_deps()

    OO_EMAIL, OO_PASSWORD = detect_oo_credentials()
    print(f"  Using OO credentials: {BOLD}{OO_EMAIL}{RESET}")

    start_openobserve(OO_EMAIL, OO_PASSWORD, force=args.force_recreate)
    wait_for_oo()

    # Auth sanity check — bail early with helpful message instead of silent 401s
    if not verify_auth(OO_EMAIL, OO_PASSWORD):
        fail("Authentication failed. Re-run with: python run.py --force-recreate")

    configure_stream(OO_EMAIL, OO_PASSWORD)
    push_logs(OO_EMAIL, OO_PASSWORD)
    ingest_metrics()
    run_evaluation()
    launch_dashboard()

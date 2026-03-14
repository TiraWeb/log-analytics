"""Wipe the database and re-run the full pipeline.

Two modes:
  python reset_and_run.py          # synthetic mode (no CSVs needed)
  python reset_and_run.py --real   # real-logs mode (reads data/raw_logs/*.csv)

In real-logs mode the pipeline order is:
  1. Ingest CSV logs  → error_rate / latency_p95 / http_5xx_rate metrics (source='logs')
  2. Generate synthetic infra metrics (cpu, memory, asg, db_connections) for context
  3. Calculate baselines (across ALL metrics)
  4. Detect anomalies  (log metrics are primary trigger)
  5. Diagnose root causes (cross-correlates log + infra signals)
"""
import os
import sys
import subprocess
from pathlib import Path

DB_PATH = Path("data/analytics.db")
REAL_LOGS_DIR = Path("data/raw_logs")

# Parse --real flag
real_mode = "--real" in sys.argv

# ── Delete existing database ───────────────────────────────────────────────
if DB_PATH.exists():
    DB_PATH.unlink()
    print(f"[reset] Deleted {DB_PATH}")
else:
    print(f"[reset] No database at {DB_PATH} — starting fresh")

# ── Build pipeline steps ───────────────────────────────────────────────────
if real_mode:
    csv_files = list(REAL_LOGS_DIR.glob("*.csv")) if REAL_LOGS_DIR.exists() else []
    if not csv_files:
        print(f"[warn] --real flag set but no CSV files found in {REAL_LOGS_DIR}/")
        print("       Falling back to synthetic-only mode.")
        real_mode = False
    else:
        print(f"[info] Real-logs mode — found {len(csv_files)} CSV file(s) in {REAL_LOGS_DIR}/")

if real_mode:
    steps = [
        ("Ingest real CSV logs",      [sys.executable, "src/ingestion/ingest_logs.py"]),
        ("Generate synthetic infra metrics",
                                       [sys.executable, "src/ingestion/generate_metrics.py"]),
        ("Calculate baselines",        [sys.executable, "src/analysis/calculate_baselines.py"]),
        ("Detect anomalies",           [sys.executable, "src/analysis/detect_anomalies.py"]),
        ("Diagnose root causes",       [sys.executable, "src/analysis/diagnose_root_cause.py"]),
    ]
else:
    steps = [
        ("Generate synthetic metrics", [sys.executable, "src/ingestion/generate_metrics.py"]),
        ("Calculate baselines",        [sys.executable, "src/analysis/calculate_baselines.py"]),
        ("Detect anomalies",           [sys.executable, "src/analysis/detect_anomalies.py"]),
        ("Diagnose root causes",       [sys.executable, "src/analysis/diagnose_root_cause.py"]),
    ]

# ── Run each step ──────────────────────────────────────────────────────────
for label, cmd in steps:
    print(f"\n{'='*60}")
    print(f"STEP: {label}")
    print('='*60)
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"\n[ERROR] '{label}' failed (exit {result.returncode})")
        sys.exit(result.returncode)

mode_label = "real-logs" if real_mode else "synthetic"
print("\n" + "="*60)
print(f"Pipeline complete! [{mode_label} mode]")
print("Launch dashboard:")
print("    python -m streamlit run src/dashboard/app.py")
print("="*60)

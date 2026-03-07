"""Helper script: wipe the database and re-run the full pipeline.

Use this any time you want a clean slate:
    python reset_and_run.py
"""
import os
import sys
import subprocess
from pathlib import Path

DB_PATH = Path("data/analytics.db")

# 1. Delete existing database
if DB_PATH.exists():
    DB_PATH.unlink()
    print(f"[reset] Deleted {DB_PATH}")
else:
    print(f"[reset] No database found at {DB_PATH} — starting fresh")

# 2. Run pipeline steps in order
steps = [
    ("Generate synthetic metrics",  [sys.executable, "src/ingestion/generate_metrics.py"]),
    ("Calculate baselines",          [sys.executable, "src/analysis/calculate_baselines.py"]),
    ("Detect anomalies",             [sys.executable, "src/analysis/detect_anomalies.py"]),
    ("Diagnose root causes",         [sys.executable, "src/analysis/diagnose_root_cause.py"]),
]

for label, cmd in steps:
    print(f"\n{'='*60}")
    print(f"STEP: {label}")
    print('='*60)
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"\n[ERROR] '{label}' failed with exit code {result.returncode}")
        sys.exit(result.returncode)

print("\n" + "="*60)
print("Pipeline complete! Launch the dashboard with:")
print("    python -m streamlit run src/dashboard/app.py")
print("="*60)

"""Wipe the database and re-run the full pipeline.

Modes:
  python reset_and_run.py            # synthetic mode (no CSVs needed)
  python reset_and_run.py --real     # real-logs mode (reads data/raw_logs/*.csv)
  python reset_and_run.py --cw       # real-logs + CloudWatch infra metrics
  python reset_and_run.py --serve    # run pipeline then start the dashboard server
  python reset_and_run.py --eval     # add evaluation step at the end
  python reset_and_run.py --real --cw --eval --serve  # full production run

CloudWatch mode pipeline:
  1. Ingest CSV logs        → error_rate / latency_p95 / http_5xx_rate  (source='logs')
  2. Ingest CloudWatch      → cpu/memory/asg/db_connections              (source='cloudwatch')
  3. Calculate baselines    → across ALL metrics
  4. Detect anomalies       → log metrics primary, infra metrics for correlation
  5. Diagnose root causes   → cross-correlates log + infra signals
  6. Evaluate (optional)    → precision/recall/F1 vs ground truth
  7. Start server (optional)→ http://localhost:8050
"""
import os
import sys
import subprocess
from pathlib import Path

DB_PATH      = Path('data/analytics.db')
REAL_LOGS_DIR = Path('data/raw_logs')

# ── Parse flags ───────────────────────────────────────────────────────────────
real_mode  = '--real'  in sys.argv
cw_mode    = '--cw'    in sys.argv
serve_mode = '--serve' in sys.argv
eval_mode  = '--eval'  in sys.argv

# --cw implies --real (need logs + infra together)
if cw_mode:
    real_mode = True

# ── Delete existing database ──────────────────────────────────────────────────
if DB_PATH.exists():
    DB_PATH.unlink()
    print(f'[reset] Deleted {DB_PATH}')
else:
    print(f'[reset] No database at {DB_PATH} — starting fresh')

# ── Validate real-logs prerequisites ─────────────────────────────────────────
if real_mode:
    csv_files = list(REAL_LOGS_DIR.glob('*.csv')) if REAL_LOGS_DIR.exists() else []
    if not csv_files:
        print(f'[warn] --real flag set but no CSV files found in {REAL_LOGS_DIR}/')
        print('       Falling back to synthetic-only mode.')
        real_mode = False
        cw_mode   = False
    else:
        print(f'[info] Real-logs mode — found {len(csv_files)} CSV file(s)')

if cw_mode:
    try:
        import boto3
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f'[info] AWS credentials OK — account={identity["Account"]}')
    except Exception as e:
        print(f'[warn] CloudWatch mode requested but AWS credentials not available: {e}')
        print('       Falling back to synthetic infra metrics.')
        cw_mode = False

# ── Build pipeline steps ──────────────────────────────────────────────────────
if real_mode and cw_mode:
    steps = [
        ('Ingest real CSV logs',           [sys.executable, 'src/ingestion/ingest_logs.py']),
        ('Ingest CloudWatch metrics',      [sys.executable, 'src/ingestion/cloudwatch_ingest.py']),
        ('Calculate baselines',            [sys.executable, 'src/analysis/calculate_baselines.py']),
        ('Detect anomalies',               [sys.executable, 'src/analysis/detect_anomalies.py']),
        ('Diagnose root causes',           [sys.executable, 'src/analysis/diagnose_root_cause.py']),
    ]
elif real_mode:
    steps = [
        ('Ingest real CSV logs',           [sys.executable, 'src/ingestion/ingest_logs.py']),
        ('Generate synthetic infra metrics', [sys.executable, 'src/ingestion/generate_metrics.py']),
        ('Calculate baselines',            [sys.executable, 'src/analysis/calculate_baselines.py']),
        ('Detect anomalies',               [sys.executable, 'src/analysis/detect_anomalies.py']),
        ('Diagnose root causes',           [sys.executable, 'src/analysis/diagnose_root_cause.py']),
    ]
else:
    steps = [
        ('Generate synthetic metrics',     [sys.executable, 'src/ingestion/generate_metrics.py']),
        ('Calculate baselines',            [sys.executable, 'src/analysis/calculate_baselines.py']),
        ('Detect anomalies',               [sys.executable, 'src/analysis/detect_anomalies.py']),
        ('Diagnose root causes',           [sys.executable, 'src/analysis/diagnose_root_cause.py']),
    ]

if eval_mode:
    steps.append(
        ('Evaluate precision/recall',      [sys.executable, 'src/analysis/evaluate.py'])
    )

# ── Run each step ─────────────────────────────────────────────────────────────
for label, cmd in steps:
    print(f'\n{"="*60}')
    print(f'STEP: {label}')
    print('='*60)
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f'\n[ERROR] \'{label}\' failed (exit {result.returncode})')
        sys.exit(result.returncode)

mode_parts = []
if real_mode:  mode_parts.append('real-logs')
if cw_mode:    mode_parts.append('cloudwatch')
if eval_mode:  mode_parts.append('eval')
if not mode_parts: mode_parts.append('synthetic')
mode_label = ' + '.join(mode_parts)

print('\n' + '='*60)
print(f'Pipeline complete! [{mode_label} mode]')
print('Dashboard: http://localhost:8050')
print('='*60)

# ── Start dashboard server (optional) ────────────────────────────────────────
if serve_mode:
    print('\nStarting dashboard server...')
    subprocess.run([sys.executable, 'src/dashboard/server.py'])

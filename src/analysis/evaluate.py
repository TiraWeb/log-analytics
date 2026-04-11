"""Precision / Recall / F1 evaluation of the anomaly detector.

Compares detected incidents against a ground-truth manifest.
Outputs a JSON file at data/evaluation_results.json consumed by the dashboard.

Ground truth is defined two ways:
  1. Auto-inferred from synthetic mode (the two known injected incidents)
  2. Manual manifest file at data/ground_truth.json (for real logs)

Manual manifest format:
    [
      {
        "service_name": "External-core-ms-2",
        "incident_type": "db_saturation",
        "window_start": "2024-03-01T05:00:00",
        "window_end":   "2024-03-01T07:00:00"
      },
      ...
    ]

Usage:
    python src/analysis/evaluate.py
    python src/analysis/evaluate.py --ground-truth data/my_labels.json
"""
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ── Synthetic ground truth (always available in synthetic mode) ───────────────
SYNTHETIC_GROUND_TRUTH = [
    {
        'service_name': 'External-core-ms-2',
        'incident_type': 'db_saturation',
        'window_start': '2024-03-01T05:30:00',
        'window_end':   '2024-03-01T07:00:00',
        'label': 'DB saturation (GT1)',
    },
    {
        'service_name': 'Internal-core-ms',
        'incident_type': 'asg_capacity_limit',
        'window_start': '2024-03-01T11:30:00',
        'window_end':   '2024-03-01T13:00:00',
        'label': 'ASG capacity limit (GT2)',
    },
]


def load_ground_truth(path: Path) -> List[Dict]:
    """Load ground truth from JSON file, or fall back to synthetic GT."""
    if path.exists():
        with open(path) as f:
            gt = json.load(f)
        logger.info(f'Loaded {len(gt)} ground truth entries from {path}')
        return gt
    logger.info('No ground_truth.json found — using synthetic ground truth (2 incidents)')
    return SYNTHETIC_GROUND_TRUTH


def incident_hits_gt(incident: Dict, gt: Dict, tolerance_minutes: int = 30) -> bool:
    """Return True if incident timestamp falls within ground-truth window + tolerance."""
    if incident['service_name'] != gt['service_name']:
        return False

    try:
        ts  = datetime.fromisoformat(incident['timestamp'])
        ws  = datetime.fromisoformat(gt['window_start']) - timedelta(minutes=tolerance_minutes)
        we  = datetime.fromisoformat(gt['window_end'])   + timedelta(minutes=tolerance_minutes)
        return ws <= ts <= we
    except (ValueError, TypeError):
        return False


def evaluate(
    detected: List[Dict],
    ground_truth: List[Dict],
    tolerance_minutes: int = 30,
) -> Dict:
    """Compute TP / FP / FN and derive precision, recall, F1.

    Args:
        detected: list of incident rows from SQLite
        ground_truth: list of GT dicts with window_start/window_end
        tolerance_minutes: how many minutes either side of the GT window
                           an incident timestamp can fall and still count as TP

    Returns:
        dict with per-GT breakdown and aggregate scores
    """
    matched_gt  = set()   # indices of GT items that were hit
    tp_incidents = []
    fp_incidents = []

    for inc in detected:
        matched = False
        for i, gt in enumerate(ground_truth):
            if incident_hits_gt(inc, gt, tolerance_minutes):
                matched_gt.add(i)
                matched = True
                tp_incidents.append({'incident': inc, 'gt_label': gt.get('label', f'GT{i+1)')})
                break
        if not matched:
            fp_incidents.append(inc)

    fn_gt = [gt for i, gt in enumerate(ground_truth) if i not in matched_gt]

    tp = len(matched_gt)
    fp = len(fp_incidents)
    fn = len(fn_gt)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0.0)

    # Per-service breakdown
    all_services = sorted({i['service_name'] for i in detected} |
                          {g['service_name'] for g in ground_truth})
    per_service = {}
    for svc in all_services:
        svc_det = [i for i in detected      if i['service_name'] == svc]
        svc_gt  = [g for g in ground_truth  if g['service_name'] == svc]
        svc_tp  = sum(1 for i in svc_det if any(
            incident_hits_gt(i, g, tolerance_minutes) for g in svc_gt
        ))
        svc_fp  = len(svc_det) - svc_tp
        svc_fn  = len(svc_gt) - svc_tp
        svc_p   = svc_tp / (svc_tp + svc_fp) if (svc_tp + svc_fp) > 0 else 0.0
        svc_r   = svc_tp / (svc_tp + svc_fn) if (svc_tp + svc_fn) > 0 else 0.0
        svc_f1  = (2 * svc_p * svc_r / (svc_p + svc_r)
                   if (svc_p + svc_r) > 0 else 0.0)
        per_service[svc] = {
            'tp': svc_tp, 'fp': svc_fp, 'fn': svc_fn,
            'precision': round(svc_p, 4),
            'recall':    round(svc_r, 4),
            'f1':        round(svc_f1, 4),
        }

    return {
        'generated_at':     datetime.now().isoformat(),
        'tolerance_minutes': tolerance_minutes,
        'ground_truth_count': len(ground_truth),
        'detected_count':   len(detected),
        'tp': tp, 'fp': fp, 'fn': fn,
        'precision':  round(precision, 4),
        'recall':     round(recall, 4),
        'f1':         round(f1, 4),
        'per_service': per_service,
        'true_positives':  tp_incidents,
        'false_positives': fp_incidents,
        'false_negatives': fn_gt,
    }


def main():
    parser = argparse.ArgumentParser(description='Evaluate anomaly detector')
    parser.add_argument(
        '--ground-truth', default='data/ground_truth.json',
        help='Path to ground truth JSON (default: data/ground_truth.json)'
    )
    parser.add_argument(
        '--tolerance', type=int, default=30,
        help='Matching tolerance in minutes (default 30)'
    )
    args = parser.parse_args()

    db        = DatabaseManager()
    detected  = db.execute_query(
        'SELECT id, timestamp, service_name, incident_type, severity, '
        'confidence_score, resolved FROM incidents ORDER BY timestamp'
    )

    if not detected:
        logger.error('No incidents in database — run the pipeline first')
        return 1

    gt_path     = Path(args.ground_truth)
    ground_truth = load_ground_truth(gt_path)

    results = evaluate(detected, ground_truth, args.tolerance)

    logger.info('\n=== EVALUATION RESULTS ===')
    logger.info(f'Ground truth incidents : {results["ground_truth_count"]}')
    logger.info(f'Detected incidents     : {results["detected_count"]}')
    logger.info(f'TP={results["tp"]}  FP={results["fp"]}  FN={results["fn"]}')
    logger.info(f'Precision : {results["precision"]:.4f}')
    logger.info(f'Recall    : {results["recall"]:.4f}')
    logger.info(f'F1        : {results["f1"]:.4f}')

    out_path = Path('data/evaluation_results.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f'\nSaved to {out_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())

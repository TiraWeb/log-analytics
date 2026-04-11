"""Flask API server — replaces Streamlit entirely.

Runs on http://localhost:8050 and serves:
  GET /api/incidents      — filtered incident list
  GET /api/incidents/<id> — single incident with metric window
  GET /api/metrics        — raw metric series for a service/metric
  GET /api/baselines      — all baselines
  GET /api/summary        — KPI counts
  GET /api/services       — distinct service names
  GET /                   — serves dashboard.html

Start with:
    python src/dashboard/server.py
or via the helper:
    python reset_and_run.py --serve
"""
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

from flask import Flask, jsonify, request, send_from_directory, abort

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=str(Path(__file__).parent))

# ── DB singleton ────────────────────────────────────────────────────────────
_db = None

def get_db() -> DatabaseManager:
    global _db
    if _db is None:
        _db = DatabaseManager()
    return _db


# ── CORS helper ─────────────────────────────────────────────────────────────
@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


# ── Static: serve the dashboard HTML ────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory(str(Path(__file__).parent), 'dashboard.html')


# ── /api/summary ─────────────────────────────────────────────────────────────
@app.route('/api/summary')
def summary():
    db = get_db()
    rows = db.execute_query("""
        SELECT
            COUNT(*)                                      AS total,
            SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical,
            SUM(CASE WHEN severity='high'     THEN 1 ELSE 0 END) AS high,
            SUM(CASE WHEN severity='medium'   THEN 1 ELSE 0 END) AS medium,
            SUM(CASE WHEN severity='low'      THEN 1 ELSE 0 END) AS low,
            SUM(CASE WHEN resolved=1          THEN 1 ELSE 0 END) AS resolved,
            AVG(confidence_score)                         AS avg_confidence
        FROM incidents
    """)
    row = rows[0] if rows else {}

    # Service health: number of open incidents per service
    svc_rows = db.execute_query("""
        SELECT service_name,
               COUNT(*) as open_incidents,
               MAX(severity) as worst_severity
        FROM incidents
        WHERE resolved = 0
        GROUP BY service_name
        ORDER BY open_incidents DESC
    """)

    # Recent trend: incidents per hour for last 24h
    trend = db.execute_query("""
        SELECT strftime('%Y-%m-%dT%H:00:00', timestamp) AS hour,
               COUNT(*) AS count
        FROM incidents
        WHERE timestamp >= datetime('now', '-24 hours')
        GROUP BY hour
        ORDER BY hour
    """)

    # Metric source breakdown
    sources = db.execute_query("""
        SELECT source, COUNT(*) as n FROM metrics GROUP BY source
    """)

    return jsonify({
        'totals': dict(row),
        'services': svc_rows,
        'trend': trend,
        'metric_sources': sources,
    })


# ── /api/services ────────────────────────────────────────────────────────────
@app.route('/api/services')
def services():
    db = get_db()
    rows = db.execute_query(
        "SELECT DISTINCT service_name FROM incidents ORDER BY service_name"
    )
    return jsonify([r['service_name'] for r in rows])


# ── /api/incidents ───────────────────────────────────────────────────────────
@app.route('/api/incidents')
def incidents():
    db  = get_db()
    svc = request.args.get('service', '')
    sev = request.args.get('severity', '')
    sta = request.args.get('status', '')    # 'open' | 'resolved' | ''
    lim = min(int(request.args.get('limit', 200)), 500)

    where, params = [], []
    if svc: where.append('service_name = ?');  params.append(svc)
    if sev: where.append('severity = ?');       params.append(sev)
    if sta == 'open':     where.append('resolved = 0')
    elif sta == 'resolved': where.append('resolved = 1')

    wc = ' AND '.join(where) if where else '1=1'
    rows = db.execute_query(
        f"""SELECT id, timestamp, service_name, incident_type, severity,
                   description, root_cause, affected_metrics,
                   confidence_score, resolved, created_at
            FROM incidents WHERE {wc}
            ORDER BY timestamp DESC LIMIT {lim}""",
        tuple(params)
    )
    return jsonify(rows)


# ── /api/incidents/<id> ───────────────────────────────────────────────────────
@app.route('/api/incidents/<int:inc_id>')
def incident_detail(inc_id):
    db   = get_db()
    rows = db.execute_query(
        'SELECT * FROM incidents WHERE id = ?', (inc_id,)
    )
    if not rows:
        abort(404)
    inc = rows[0]

    # Metric window ±90 minutes
    metrics = db.execute_query(
        """
        SELECT timestamp, metric_name, metric_value, source
        FROM   metrics
        WHERE  service_name = ?
        AND    timestamp >= strftime('%Y-%m-%dT%H:%M:%S', datetime(?, '-90 minutes'))
        AND    timestamp <= strftime('%Y-%m-%dT%H:%M:%S', datetime(?, '+90 minutes'))
        ORDER  BY timestamp
        """,
        (inc['service_name'], inc['timestamp'], inc['timestamp'])
    )

    # Baselines for this service
    baselines = db.execute_query(
        """
        SELECT metric_name, mean, stddev, p95, p99, sample_count
        FROM   baselines
        WHERE  service_name = ?
        ORDER  BY created_at DESC
        """,
        (inc['service_name'],)
    )
    # Deduplicate — keep latest per metric_name
    seen, bl_dedup = set(), []
    for b in baselines:
        if b['metric_name'] not in seen:
            bl_dedup.append(b)
            seen.add(b['metric_name'])

    return jsonify({
        'incident': inc,
        'metrics':  metrics,
        'baselines': bl_dedup,
    })


# ── /api/metrics ──────────────────────────────────────────────────────────────
@app.route('/api/metrics')
def metrics():
    db      = get_db()
    svc     = request.args.get('service', '')
    metric  = request.args.get('metric', '')
    hours   = int(request.args.get('hours', 24))

    where, params = [], []
    if svc:    where.append('service_name = ?');  params.append(svc)
    if metric: where.append('metric_name = ?');   params.append(metric)
    where.append(f"timestamp >= datetime('now', '-{hours} hours')")

    wc = ' AND '.join(where)
    rows = db.execute_query(
        f"SELECT timestamp, service_name, metric_name, metric_value, source "
        f"FROM metrics WHERE {wc} ORDER BY timestamp",
        tuple(params)
    )
    return jsonify(rows)


# ── /api/baselines ────────────────────────────────────────────────────────────
@app.route('/api/baselines')
def baselines():
    db  = get_db()
    svc = request.args.get('service', '')

    where, params = [], []
    if svc: where.append('service_name = ?'); params.append(svc)
    wc = ' AND '.join(where) if where else '1=1'

    rows = db.execute_query(
        f"SELECT service_name, metric_name, mean, stddev, p50, p95, p99, "
        f"sample_count, window_start, window_end "
        f"FROM baselines WHERE {wc} ORDER BY service_name, metric_name",
        tuple(params)
    )
    # Deduplicate — latest per (service, metric)
    seen, out = set(), []
    for r in rows:
        key = (r['service_name'], r['metric_name'])
        if key not in seen:
            out.append(r)
            seen.add(key)
    return jsonify(out)


# ── /api/evaluate ─────────────────────────────────────────────────────────────
@app.route('/api/evaluate')
def evaluate():
    """Return evaluation metrics if the evaluate.py output exists."""
    eval_path = Path('data/evaluation_results.json')
    if not eval_path.exists():
        return jsonify({'error': 'No evaluation results — run: python src/analysis/evaluate.py'}), 404
    with open(eval_path) as f:
        return jsonify(json.load(f))


# ── /api/resolve/<id> (POST) ──────────────────────────────────────────────────
@app.route('/api/resolve/<int:inc_id>', methods=['POST'])
def resolve(inc_id):
    db = get_db()
    db.execute_insert(
        "UPDATE incidents SET resolved=1, resolved_at=? WHERE id=?",
        (datetime.now().isoformat(), inc_id)
    )
    return jsonify({'ok': True, 'id': inc_id})


if __name__ == '__main__':
    logger.info('Starting Log Analytics dashboard on http://localhost:8050')
    app.run(host='0.0.0.0', port=8050, debug=False)

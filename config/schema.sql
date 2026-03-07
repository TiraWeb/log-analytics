-- Incidents table: stores detected anomalies and their root causes
CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    service_name TEXT NOT NULL,
    incident_type TEXT NOT NULL,  -- 'error_spike', 'latency_spike', 'resource_anomaly'
    severity TEXT NOT NULL,       -- 'low', 'medium', 'high', 'critical'
    description TEXT,
    root_cause TEXT,
    affected_metrics TEXT,        -- JSON string of affected metric names
    confidence_score REAL,        -- 0.0 to 1.0
    resolved BOOLEAN DEFAULT 0,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE(timestamp, service_name, incident_type)
);

-- Baselines table: stores statistical baselines for normal behavior
CREATE TABLE IF NOT EXISTS baselines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    mean REAL,
    stddev REAL,
    p50 REAL,
    p95 REAL,
    p99 REAL,
    sample_count INTEGER,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(service_name, metric_name, window_start)
);

-- Metrics table: stores aggregated metrics from logs and infrastructure
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    service_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    metric_type TEXT NOT NULL,    -- 'error_rate', 'latency', 'cpu', 'memory', 'db_connections'
    source TEXT NOT NULL,         -- 'logs', 'cloudwatch', 'synthetic'
    created_at TEXT NOT NULL,
    UNIQUE(timestamp, service_name, metric_name)
);

-- Query log table: tracks analysis queries for debugging
CREATE TABLE IF NOT EXISTS query_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_type TEXT NOT NULL,
    query_text TEXT NOT NULL,
    execution_time_ms REAL,
    result_count INTEGER,
    error TEXT,
    created_at TEXT NOT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_incidents_timestamp ON incidents(timestamp);
CREATE INDEX IF NOT EXISTS idx_incidents_service ON incidents(service_name);
CREATE INDEX IF NOT EXISTS idx_baselines_service ON baselines(service_name);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_service ON metrics(service_name);
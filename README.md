# Log Analytics Platform

**Intelligent Log Analytics and Automated Root Cause Diagnosis for Microservices**

A production-ready log analytics platform that automatically detects anomalies and diagnoses root causes in distributed microservice architectures. Built for Velaris.io's AWS ECS infrastructure.

## 🎯 Features

- **Automated Log Ingestion**: Parse and ingest logs from 30+ microservices and event handlers
- **Statistical Baseline Calculation**: Compute normal behavior baselines for all metrics
- **Anomaly Detection**: Z-score based detection with configurable thresholds
- **Root Cause Diagnosis**: Pattern matching and correlation analysis
- **Interactive Dashboard**: Real-time visualization of incidents and metrics
- **Ground Truth Evaluation**: Synthetic incidents for academic validation

## 🏗️ Architecture

```
log-analytics-platform/
├── config/               # Configuration files
│   ├── config.yaml      # Main configuration
│   └── schema.sql       # Database schema
├── data/
│   ├── raw_logs/        # CSV log files
│   ├── metrics/         # Generated metrics
│   └── openobserve/     # OpenObserve data
├── src/
│   ├── utils/           # Utility modules
│   │   ├── log_parser.py
│   │   ├── db_utils.py
│   │   └── openobserve_client.py
│   ├── ingestion/       # Data ingestion
│   │   ├── ingest_logs.py
│   │   └── generate_metrics.py
│   ├── analysis/        # Analysis modules
│   │   ├── calculate_baselines.py
│   │   ├── detect_anomalies.py
│   │   └── diagnose_root_cause.py
│   └── dashboard/       # Streamlit dashboard
│       └── app.py
└── tests/               # Unit tests
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker Desktop
- Git

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/TiraWeb/log-analytics.git
cd log-analytics
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Create data directories**

```bash
mkdir -p data/raw_logs data/metrics data/openobserve
```

4. **Start OpenObserve**

```bash
docker-compose up -d
```

Wait for OpenObserve to be healthy:
```bash
docker-compose ps
```

### Usage

#### Step 1: Generate Synthetic Metrics

Generate 24 hours of synthetic metrics with 2 ground truth incidents:

```bash
python src/ingestion/generate_metrics.py
```

This creates:
- **Incident 1 (T+6h)**: DB saturation in External-core-ms-2
- **Incident 2 (T+12h)**: ASG capacity limit in Internal-core-ms

#### Step 2: Ingest Logs (Optional)

Copy your CSV log files to `data/raw_logs/`, then:

```bash
python src/ingestion/ingest_logs.py
```

Supported log format:
```
level|correlation_id|tenant_id|[req_id|]timestamp:	log_text
```

#### Step 3: Calculate Baselines

Compute statistical baselines for normal behavior:

```bash
python src/analysis/calculate_baselines.py
```

#### Step 4: Detect Anomalies

Run anomaly detection:

```bash
python src/analysis/detect_anomalies.py
```

#### Step 5: Diagnose Root Causes

Diagnose incidents:

```bash
python src/analysis/diagnose_root_cause.py
```

#### Step 6: Launch Dashboard (Coming Soon)

```bash
streamlit run src/dashboard/app.py
```

## 📊 Database Schema

### Tables

- **incidents**: Detected anomalies and root causes
- **baselines**: Statistical baselines for metrics
- **metrics**: Infrastructure and application metrics
- **query_log**: Query execution tracking

## 🧪 Testing

Run unit tests:

```bash
python tests/test_core.py
```

## 📈 Evaluation

The platform includes ground truth incidents for academic evaluation:

1. **DB Saturation** (T+6h)
   - Service: External-core-ms-2
   - Duration: 30 minutes
   - Signature: DB connections 190-200 (normal: 50-150)

2. **ASG Capacity Limit** (T+12h)
   - Service: Internal-core-ms
   - Duration: 30 minutes
   - Signature: ASG at max capacity (10 instances), CPU 85-95%, Memory 80-90%

## 🛠️ Configuration

Edit `config/config.yaml` to customize:

- OpenObserve connection settings
- Anomaly detection thresholds (default: 3σ)
- Baseline calculation windows
- Correlation thresholds

## 📝 Log Format

The platform parses pipe-delimited logs with ANSI color codes:

```
\x1b[32minfo\x1b[39m|abc123|tenant1|[req123|]2024-03-01T10:00:00.000Z:\tUser logged in
```

Parsed fields:
- `level`: Log level (info, warn, error)
- `correlation_id`: Request correlation ID
- `tenant_id`: Tenant identifier
- `req_id`: Optional request ID
- `timestamp`: ISO 8601 timestamp
- `log_text`: Log message

Automatic extraction:
- HTTP status codes
- Latency from `[End Request]` lines
- Error detection by level and keywords
- Multi-line stack trace merging

## 🎓 Academic Context

This project is part of the CS6P05NM (Honours Project) module at London Metropolitan University.

**Student**: Tiran Pankaja  
**Supervisor**: Dr. Zenon Trybulec  
**Organization**: Velaris.io  

## 📄 License

MIT License - see LICENSE file for details

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Contact

- GitHub: [@TiraWeb](https://github.com/TiraWeb)
- Email: [Your Email]

## 🗺️ Roadmap

- [x] Core infrastructure
- [x] Log parsing and ingestion
- [x] Baseline calculation
- [x] Anomaly detection
- [x] Root cause diagnosis
- [ ] Interactive dashboard
- [ ] Real-time alerting
- [ ] Machine learning models
- [ ] Multi-tenancy support
- [ ] Kubernetes integration
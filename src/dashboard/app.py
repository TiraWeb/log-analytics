"""Streamlit dashboard for log analytics platform."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.db_utils import DatabaseManager
import yaml

# Page config
st.set_page_config(
    page_title="Log Analytics Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Initialize database
@st.cache_resource
def get_db():
    return DatabaseManager()

db = get_db()

# Title
st.title("📈 Log Analytics Platform")
st.markdown("**Intelligent Anomaly Detection and Root Cause Diagnosis**")

# Sidebar
st.sidebar.header("⚙️ Filters")

# Get all services
services_query = "SELECT DISTINCT service_name FROM incidents ORDER BY service_name"
services_result = db.execute_query(services_query)
all_services = ['All'] + [s['service_name'] for s in services_result]

selected_service = st.sidebar.selectbox("Service", all_services)

# Severity filter
severity_options = ['All', 'critical', 'high', 'medium', 'low']
selected_severity = st.sidebar.selectbox("Severity", severity_options)

# Status filter
status_options = ['All', 'Unresolved', 'Resolved']
selected_status = st.sidebar.selectbox("Status", status_options)

# Refresh button
if st.sidebar.button("🔄 Refresh"):
    st.rerun()

# Build query
where_clauses = []
params = []

if selected_service != 'All':
    where_clauses.append("service_name = ?")
    params.append(selected_service)

if selected_severity != 'All':
    where_clauses.append("severity = ?")
    params.append(selected_severity)

if selected_status == 'Unresolved':
    where_clauses.append("resolved = 0")
elif selected_status == 'Resolved':
    where_clauses.append("resolved = 1")

where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

# Get incidents
incidents_query = f"""
SELECT id, timestamp, service_name, incident_type, severity, 
       description, root_cause, confidence_score, resolved
FROM incidents
WHERE {where_clause}
ORDER BY timestamp DESC
LIMIT 100
"""

incidents = db.execute_query(incidents_query, tuple(params))

if not incidents:
    st.warning("⚠️ No incidents found. Run the analysis pipeline first:")
    st.code("""
python src/ingestion/generate_metrics.py
python src/analysis/calculate_baselines.py
python src/analysis/detect_anomalies.py
python src/analysis/diagnose_root_cause.py
""")
    st.stop()

df_incidents = pd.DataFrame(incidents)

# Summary metrics
st.header("📊 Summary")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Incidents", len(df_incidents))

with col2:
    critical_count = len(df_incidents[df_incidents['severity'] == 'critical'])
    st.metric("Critical", critical_count, delta="Urgent" if critical_count > 0 else None, delta_color="inverse")

with col3:
    resolved_count = len(df_incidents[df_incidents['resolved'] == 1])
    st.metric("Resolved", resolved_count)

with col4:
    avg_confidence = df_incidents['confidence_score'].mean() * 100 if 'confidence_score' in df_incidents else 0
    st.metric("Avg Confidence", f"{avg_confidence:.1f}%")

# Severity distribution
st.subheader("📊 Severity Distribution")

severity_counts = df_incidents['severity'].value_counts()
fig_severity = px.pie(
    values=severity_counts.values,
    names=severity_counts.index,
    title="Incidents by Severity",
    color=severity_counts.index,
    color_discrete_map={
        'critical': '#ff4444',
        'high': '#ff8800',
        'medium': '#ffcc00',
        'low': '#44ff44'
    }
)
st.plotly_chart(fig_severity, use_container_width=True)

# Incidents over time
st.subheader("📈 Incidents Over Time")

df_incidents['timestamp_dt'] = pd.to_datetime(df_incidents['timestamp'])
df_timeline = df_incidents.groupby(
    [pd.Grouper(key='timestamp_dt', freq='1H'), 'severity']
).size().reset_index(name='count')

fig_timeline = px.line(
    df_timeline,
    x='timestamp_dt',
    y='count',
    color='severity',
    title="Incident Timeline (Hourly)",
    labels={'timestamp_dt': 'Time', 'count': 'Number of Incidents'},
    color_discrete_map={
        'critical': '#ff4444',
        'high': '#ff8800',
        'medium': '#ffcc00',
        'low': '#44ff44'
    }
)
st.plotly_chart(fig_timeline, use_container_width=True)

# Incidents by service
if selected_service == 'All':
    st.subheader("💻 Incidents by Service")
    
    service_counts = df_incidents['service_name'].value_counts()
    fig_services = px.bar(
        x=service_counts.index,
        y=service_counts.values,
        title="Incidents per Service",
        labels={'x': 'Service', 'y': 'Number of Incidents'},
        color=service_counts.values,
        color_continuous_scale='Reds'
    )
    st.plotly_chart(fig_services, use_container_width=True)

# Incident details table
st.header("📝 Incident Details")

# Format for display
df_display = df_incidents[[
    'id', 'timestamp', 'service_name', 'incident_type', 
    'severity', 'description', 'root_cause', 'confidence_score'
]].copy()

df_display['confidence_score'] = (df_display['confidence_score'] * 100).round(1).astype(str) + '%'
df_display['root_cause'] = df_display['root_cause'].fillna('Not diagnosed yet')

# Apply styling
def highlight_severity(row):
    if row['severity'] == 'critical':
        return ['background-color: #ffcccc'] * len(row)
    elif row['severity'] == 'high':
        return ['background-color: #ffe6cc'] * len(row)
    elif row['severity'] == 'medium':
        return ['background-color: #ffffcc'] * len(row)
    else:
        return [''] * len(row)

styled_df = df_display.style.apply(highlight_severity, axis=1)

st.dataframe(styled_df, use_container_width=True, height=400)

# Expandable incident details
st.subheader("🔍 Detailed View")

selected_incident_id = st.selectbox(
    "Select Incident",
    df_incidents['id'].tolist(),
    format_func=lambda x: f"Incident #{x} - {df_incidents[df_incidents['id']==x]['service_name'].values[0]}"
)

if selected_incident_id:
    incident = df_incidents[df_incidents['id'] == selected_incident_id].iloc[0]
    
    with st.expander(f"🔴 Incident #{selected_incident_id} Details", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Service:** {incident['service_name']}")
            st.markdown(f"**Type:** {incident['incident_type']}")
            st.markdown(f"**Severity:** {incident['severity'].upper()}")
            st.markdown(f"**Timestamp:** {incident['timestamp']}")
        
        with col2:
            st.markdown(f"**Confidence:** {incident['confidence_score']*100:.1f}%")
            st.markdown(f"**Status:** {'Resolved ✅' if incident['resolved'] else 'Unresolved ⚠️'}")
        
        st.markdown("---")
        st.markdown(f"**Description:**")
        st.info(incident['description'])
        
        if incident['root_cause']:
            st.markdown(f"**Root Cause Analysis:**")
            st.success(incident['root_cause'])
        else:
            st.warning("⚠️ Root cause not yet diagnosed. Run: `python src/analysis/diagnose_root_cause.py`")
        
        # Get related metrics
        st.markdown("**Related Metrics:**")
        
        metrics_query = """
        SELECT timestamp, metric_name, metric_value
        FROM metrics
        WHERE service_name = ?
        AND timestamp BETWEEN datetime(?, '-1 hour') AND datetime(?, '+1 hour')
        ORDER BY timestamp
        """
        
        metrics = db.execute_query(
            metrics_query,
            (incident['service_name'], incident['timestamp'], incident['timestamp'])
        )
        
        if metrics:
            df_metrics = pd.DataFrame(metrics)
            df_metrics['timestamp_dt'] = pd.to_datetime(df_metrics['timestamp'])
            
            # Plot metrics around incident time
            fig_metrics = px.line(
                df_metrics,
                x='timestamp_dt',
                y='metric_value',
                color='metric_name',
                title=f"Metrics around incident time (±1 hour)",
                labels={'timestamp_dt': 'Time', 'metric_value': 'Value'}
            )
            
            # Add vertical line at incident time
            incident_time = pd.to_datetime(incident['timestamp'])
            fig_metrics.add_vline(
                x=incident_time,
                line_dash="dash",
                line_color="red",
                annotation_text="Incident"
            )
            
            st.plotly_chart(fig_metrics, use_container_width=True)
        else:
            st.info("No metrics found for this time period")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 0.9em;'>
    <p>Log Analytics Platform | Built with Streamlit | 🔗 <a href='https://github.com/TiraWeb/log-analytics'>GitHub</a></p>
    </div>
    """,
    unsafe_allow_html=True
)
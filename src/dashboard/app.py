"""Streamlit dashboard — modern redesign."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.db_utils import DatabaseManager
import yaml

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Log Analytics",
    page_icon="🟠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.8rem; padding-bottom: 2rem; }

.metric-card {
    background: #1e2130; border-radius: 12px;
    padding: 1.2rem 1.4rem; border-left: 4px solid; margin-bottom: 0.5rem;
}
.metric-card.total { border-color: #4f8ef7; }
.metric-card.crit  { border-color: #f74f4f; }
.metric-card.ok    { border-color: #4fcf70; }
.metric-card.conf  { border-color: #f7b74f; }
.metric-label { color: #8b95a8; font-size: 0.78rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 4px; }
.metric-value { color: #e8eaf0; font-size: 2rem; font-weight: 700; line-height: 1.1; }
.metric-sub   { color: #8b95a8; font-size: 0.78rem; margin-top: 4px; }

.inc-card {
    background: #1e2130; border-radius: 10px;
    padding: 1rem 1.2rem; margin-bottom: 0.7rem; border-left: 4px solid;
}
.inc-card.critical { border-color: #f74f4f; }
.inc-card.high     { border-color: #f79a4f; }
.inc-card.medium   { border-color: #f7e04f; }
.inc-card.low      { border-color: #4fcf70; }

.badge {
    display: inline-block; padding: 2px 10px; border-radius: 20px;
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
}
.badge.critical { background:#f74f4f22; color:#f74f4f; border:1px solid #f74f4f55; }
.badge.high     { background:#f79a4f22; color:#f79a4f; border:1px solid #f79a4f55; }
.badge.medium   { background:#f7e04f22; color:#f7e04f; border:1px solid #f7e04f55; }
.badge.low      { background:#4fcf7022; color:#4fcf70; border:1px solid #4fcf7055; }
.badge.resolved { background:#4fcf7022; color:#4fcf70; border:1px solid #4fcf7055; }
.badge.open     { background:#f74f4f22; color:#f74f4f; border:1px solid #f74f4f55; }

.section-title {
    color: #e8eaf0; font-size: 1.05rem; font-weight: 600;
    margin: 1.6rem 0 0.8rem 0; padding-bottom: 0.4rem; border-bottom: 1px solid #2a2f45;
}
.detail-panel {
    background: #1e2130; border-radius: 12px; padding: 1.4rem; margin-top: 0.5rem;
}
.detail-key { color: #8b95a8; font-size: 0.82rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.04em; }
.detail-val { color: #e8eaf0; font-size: 0.9rem; font-weight: 500; }

.info-box {
    border-radius: 8px; padding: 0.8rem 1rem;
    font-size: 0.88rem; margin: 0.5rem 0; line-height: 1.5;
}
.info-box.blue  { background:#1a3a5c; color:#7bbfff; border-left:3px solid #4f8ef7; }
.info-box.warn  { background:#3a2a1a; color:#ffcc7b; border-left:3px solid #f7b74f; }

/* ---- root cause card ---- */
.rc-card {
    background: #192a1f;
    border: 1px solid #2a4a35;
    border-left: 3px solid #4fcf70;
    border-radius: 8px;
    padding: 1rem 1.1rem;
    margin: 0.5rem 0;
}
.rc-summary {
    color: #c8f0d4;
    font-size: 0.92rem;
    font-weight: 600;
    margin-bottom: 0.9rem;
    line-height: 1.4;
}
.rc-row {
    display: flex;
    align-items: flex-start;
    gap: 0.6rem;
    margin-bottom: 0.6rem;
}
.rc-label {
    color: #5a8a68;
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    min-width: 110px;
    padding-top: 2px;
}
.rc-chip {
    display: inline-block;
    background: #1e3a28;
    color: #7bffaa;
    border: 1px solid #3a6a4a;
    border-radius: 4px;
    padding: 1px 8px;
    font-size: 0.76rem;
    font-family: 'Courier New', monospace;
    margin: 0 3px 3px 0;
}
.rc-action {
    color: #a8e6b8;
    font-size: 0.85rem;
    line-height: 1.4;
}

[data-testid="stSidebar"] { background:#161825; border-right: 1px solid #2a2f45; }
[data-testid="stSidebar"] .stSelectbox label { color: #8b95a8 !important; font-size:0.8rem; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ────────────────────────────────────────────────────────────────────
SEV_COLOR = {
    'critical': '#f74f4f',
    'high':     '#f79a4f',
    'medium':   '#f7e04f',
    'low':      '#4fcf70',
}
PLOT_BG  = '#161825'
PAPER_BG = '#161825'
GRID_COL = '#2a2f45'

def plotly_theme(fig):
    fig.update_layout(
        paper_bgcolor=PAPER_BG, plot_bgcolor=PLOT_BG,
        font=dict(color='#c8cdd8', family='Inter, sans-serif', size=12),
        margin=dict(l=10, r=10, t=36, b=10),
        legend=dict(bgcolor='rgba(0,0,0,0)', borderwidth=0),
        xaxis=dict(gridcolor=GRID_COL, zeroline=False, showline=False),
        yaxis=dict(gridcolor=GRID_COL, zeroline=False, showline=False),
    )
    return fig


def render_root_cause(text: str) -> str:
    """Parse structured root cause text into a tidy HTML card."""
    # Extract each known field with regex
    summary_match = re.match(r'^([^.]+\.)', text)
    primary_match = re.search(r'Primary metric[s]?:\s*([^.]+)', text, re.I)
    corr_match    = re.search(r'Correlated metric[s]?:\s*([^.]+)', text, re.I)
    action_match  = re.search(r'Recommended action[s]?:\s*(.+)$', text, re.I)

    summary = summary_match.group(1).strip() if summary_match else text

    def chips(raw: str) -> str:
        items = [m.strip() for m in raw.split(',') if m.strip() and m.strip().lower() != 'none']
        if not items:
            return "<span style='color:#5a8a68;font-size:0.82rem;'>None</span>"
        return ''.join(f"<span class='rc-chip'>{i}</span>" for i in items)

    primary_html = chips(primary_match.group(1)) if primary_match else "<span style='color:#5a8a68;'>N/A</span>"
    corr_html    = chips(corr_match.group(1))    if corr_match    else "<span style='color:#5a8a68;'>None</span>"
    action_text  = action_match.group(1).strip() if action_match  else ''

    rows = (
        f"<div class='rc-row'>"
        f"  <span class='rc-label'>Primary</span>"
        f"  <div>{primary_html}</div>"
        f"</div>"
        f"<div class='rc-row'>"
        f"  <span class='rc-label'>Correlated</span>"
        f"  <div>{corr_html}</div>"
        f"</div>"
    )
    if action_text:
        rows += (
            f"<div class='rc-row' style='margin-top:0.3rem;'>"
            f"  <span class='rc-label'>Action</span>"
            f"  <div class='rc-action'>{action_text}</div>"
            f"</div>"
        )

    return (
        f"<div class='rc-card'>"
        f"  <div class='rc-summary'>{summary}</div>"
        f"  {rows}"
        f"</div>"
    )


@st.cache_resource
def get_db():
    return DatabaseManager()


db = get_db()

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.markdown(
    "<div style='color:#e8eaf0;font-size:1.1rem;font-weight:700;"
    "padding:0.6rem 0 1rem 0;'>&#x2699;&#xFE0F; Filters</div>",
    unsafe_allow_html=True
)

svcs = db.execute_query("SELECT DISTINCT service_name FROM incidents ORDER BY service_name")
all_services = ['All'] + [s['service_name'] for s in svcs]
selected_service  = st.sidebar.selectbox("Service",  all_services)
selected_severity = st.sidebar.selectbox("Severity", ['All', 'critical', 'high', 'medium', 'low'])
selected_status   = st.sidebar.selectbox("Status",   ['All', 'Unresolved', 'Resolved'])

st.sidebar.markdown("<div style='margin-top:1rem'></div>", unsafe_allow_html=True)
if st.sidebar.button("&#x1F504; Refresh", use_container_width=True):
    st.rerun()

# ── Build query ────────────────────────────────────────────────────────────────
where, params = [], []
if selected_service  != 'All': where.append("service_name = ?");  params.append(selected_service)
if selected_severity != 'All': where.append("severity = ?");       params.append(selected_severity)
if selected_status == 'Unresolved': where.append("resolved = 0")
elif selected_status == 'Resolved': where.append("resolved = 1")

wc = " AND ".join(where) if where else "1=1"

incidents = db.execute_query(
    f"""SELECT id, timestamp, service_name, incident_type, severity,
               description, root_cause, confidence_score, resolved
        FROM incidents WHERE {wc} ORDER BY timestamp DESC LIMIT 200""",
    tuple(params)
)

if not incidents:
    st.markdown(
        "<div class='detail-panel'>"
        "<div style='color:#f7b74f;font-size:1rem;font-weight:600;'>&#x26A0;&#xFE0F; No incidents found</div>"
        "<div style='color:#8b95a8;margin-top:0.6rem;font-size:0.88rem;'>Run the analysis pipeline first:</div>"
        "</div>", unsafe_allow_html=True
    )
    st.code("python reset_and_run.py")
    st.stop()

df = pd.DataFrame(incidents)
df['timestamp_dt'] = pd.to_datetime(df['timestamp'])

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(
    "<div style='display:flex;align-items:baseline;gap:0.7rem;margin-bottom:0.2rem;'>"
    "<span style='font-size:1.7rem;font-weight:700;color:#e8eaf0;'>Log Analytics</span>"
    "<span style='font-size:0.9rem;color:#8b95a8;font-weight:400;'>Anomaly Detection &amp; Root Cause Diagnosis</span>"
    "</div>",
    unsafe_allow_html=True
)

# ── Summary cards ──────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>Overview</div>", unsafe_allow_html=True)

total    = len(df)
critical = len(df[df['severity'] == 'critical'])
resolved = len(df[df['resolved'] == 1])
avg_conf = df['confidence_score'].mean() * 100

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"<div class='metric-card total'><div class='metric-label'>Total Incidents</div>"
                f"<div class='metric-value'>{total}</div><div class='metric-sub'>all severities</div></div>",
                unsafe_allow_html=True)
with c2:
    st.markdown(f"<div class='metric-card crit'><div class='metric-label'>Critical</div>"
                f"<div class='metric-value'>{critical}</div>"
                f"<div class='metric-sub'>{'&#x1F534; needs attention' if critical else '&#x2705; clear'}</div></div>",
                unsafe_allow_html=True)
with c3:
    st.markdown(f"<div class='metric-card ok'><div class='metric-label'>Resolved</div>"
                f"<div class='metric-value'>{resolved}</div><div class='metric-sub'>of {total} total</div></div>",
                unsafe_allow_html=True)
with c4:
    st.markdown(f"<div class='metric-card conf'><div class='metric-label'>Avg Confidence</div>"
                f"<div class='metric-value'>{avg_conf:.0f}%</div><div class='metric-sub'>detection accuracy</div></div>",
                unsafe_allow_html=True)

# ── Charts ─────────────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>Analytics</div>", unsafe_allow_html=True)

col_left, col_right = st.columns([1, 2])
with col_left:
    sev_counts = df['severity'].value_counts()
    fig_pie = px.pie(values=sev_counts.values, names=sev_counts.index, hole=0.55,
                     color=sev_counts.index, color_discrete_map=SEV_COLOR)
    fig_pie.update_traces(textfont_size=12, marker=dict(line=dict(color=PLOT_BG, width=2)))
    fig_pie.update_layout(title=dict(text="Severity Mix", font=dict(size=13)))
    plotly_theme(fig_pie)
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    df_tl = (df.groupby([pd.Grouper(key='timestamp_dt', freq='1h'), 'severity'])
               .size().reset_index(name='count'))
    fig_tl = px.bar(df_tl, x='timestamp_dt', y='count', color='severity',
                    color_discrete_map=SEV_COLOR,
                    labels={'timestamp_dt': '', 'count': 'Incidents'}, barmode='stack')
    fig_tl.update_layout(title=dict(text="Incident Timeline", font=dict(size=13)))
    plotly_theme(fig_tl)
    st.plotly_chart(fig_tl, use_container_width=True)

if selected_service == 'All' and df['service_name'].nunique() > 1:
    svc_counts = df.groupby(['service_name', 'severity']).size().reset_index(name='count')
    fig_svc = px.bar(svc_counts, x='service_name', y='count', color='severity',
                     color_discrete_map=SEV_COLOR,
                     labels={'service_name': '', 'count': 'Incidents'}, barmode='stack')
    fig_svc.update_layout(title=dict(text="Incidents by Service", font=dict(size=13)))
    plotly_theme(fig_svc)
    st.plotly_chart(fig_svc, use_container_width=True)

# ── Incident list ──────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>Incidents</div>", unsafe_allow_html=True)

for _, row in df.iterrows():
    sev        = row['severity']
    itype      = row['incident_type'].replace('_', ' ').title()
    ts         = row['timestamp_dt'].strftime('%b %d, %Y  %H:%M')
    conf       = f"{row['confidence_score']*100:.0f}%"
    stat       = 'resolved' if row['resolved'] else 'open'
    stat_label = '&#x2705; Resolved' if row['resolved'] else '&#x26A0;&#xFE0F; Open'
    st.markdown(
        f"<div class='inc-card {sev}'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;'>"
        f"  <div><span style='color:#e8eaf0;font-weight:600;font-size:0.95rem;'>{row['service_name']}</span>"
        f"  &nbsp;&nbsp;<span class='badge {sev}'>{sev}</span>&nbsp;<span class='badge {stat}'>{stat_label}</span></div>"
        f"  <span style='color:#8b95a8;font-size:0.8rem;'>{ts}</span></div>"
        f"<div style='color:#8b95a8;font-size:0.82rem;margin-top:0.35rem;'>{itype} &middot; Confidence: {conf}</div>"
        f"</div>",
        unsafe_allow_html=True
    )

# ── Detailed view ──────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>Incident Deep Dive</div>", unsafe_allow_html=True)

selected_id = st.selectbox(
    "Select incident",
    df['id'].tolist(),
    format_func=lambda x: (
        f"#{x}  —  "
        + df.loc[df['id']==x, 'service_name'].values[0]
        + "  ("
        + df.loc[df['id']==x, 'incident_type'].values[0].replace('_',' ').title()
        + ")"
    ),
    label_visibility='collapsed'
)

if selected_id:
    inc = df[df['id'] == selected_id].iloc[0]
    sev = inc['severity']
    border_color = SEV_COLOR.get(sev, '#888')

    st.markdown(
        f"<div class='detail-panel' style='border-top:3px solid {border_color};'>"
        f"<div style='display:flex;flex-wrap:wrap;gap:2rem;margin-bottom:1rem;'>"
        f"  <div><div class='detail-key'>Service</div><div class='detail-val'>{inc['service_name']}</div></div>"
        f"  <div><div class='detail-key'>Incident Type</div><div class='detail-val'>{inc['incident_type'].replace('_',' ').title()}</div></div>"
        f"  <div><div class='detail-key'>Severity</div><div class='detail-val'><span class='badge {sev}'>{sev}</span></div></div>"
        f"  <div><div class='detail-key'>Detected At</div><div class='detail-val'>{inc['timestamp_dt'].strftime('%b %d, %Y  %H:%M')}</div></div>"
        f"  <div><div class='detail-key'>Confidence</div><div class='detail-val'>{inc['confidence_score']*100:.0f}%</div></div>"
        f"  <div><div class='detail-key'>Status</div><div class='detail-val'>{'&#x2705; Resolved' if inc['resolved'] else '&#x26A0;&#xFE0F; Unresolved'}</div></div>"
        f"</div>",
        unsafe_allow_html=True
    )

    # Description
    st.markdown(
        f"<div style='margin-bottom:0.4rem;color:#8b95a8;font-size:0.78rem;"
        f"font-weight:600;text-transform:uppercase;letter-spacing:0.05em;'>Description</div>"
        f"<div class='info-box blue'>{inc['description']}</div>",
        unsafe_allow_html=True
    )

    # Root cause — structured card
    st.markdown(
        "<div style='margin:0.8rem 0 0.4rem 0;color:#8b95a8;font-size:0.78rem;"
        "font-weight:600;text-transform:uppercase;letter-spacing:0.05em;'>Root Cause Analysis</div>",
        unsafe_allow_html=True
    )
    if inc['root_cause']:
        st.markdown(render_root_cause(inc['root_cause']), unsafe_allow_html=True)
    else:
        st.markdown(
            "<div class='info-box warn'>&#x26A0;&#xFE0F; Root cause not yet diagnosed. "
            "Run: <code>python src/analysis/diagnose_root_cause.py</code></div>",
            unsafe_allow_html=True
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Metric timeline ───────────────────────────────────────────────────
    st.markdown(
        "<div style='margin-top:1rem;color:#8b95a8;font-size:0.78rem;font-weight:600;"
        "text-transform:uppercase;letter-spacing:0.05em;margin-bottom:0.4rem;'>"
        "Metric Timeline (±1 hour around incident)</div>",
        unsafe_allow_html=True
    )

    metrics = db.execute_query(
        """
        SELECT timestamp, metric_name, metric_value
        FROM   metrics
        WHERE  service_name = ?
        AND    timestamp >= strftime('%Y-%m-%dT%H:%M:%S', datetime(?, '-60 minutes'))
        AND    timestamp <= strftime('%Y-%m-%dT%H:%M:%S', datetime(?, '+60 minutes'))
        ORDER  BY timestamp
        """,
        (inc['service_name'], inc['timestamp'], inc['timestamp'])
    )

    if metrics:
        df_m = pd.DataFrame(metrics)
        df_m['timestamp_dt'] = pd.to_datetime(df_m['timestamp'])
        fig_m = px.line(df_m, x='timestamp_dt', y='metric_value', color='metric_name',
                        labels={'timestamp_dt': '', 'metric_value': 'Value', 'metric_name': 'Metric'},
                        line_shape='spline')
        inc_x = inc['timestamp_dt'].isoformat()
        fig_m.add_shape(type='line', x0=inc_x, x1=inc_x, y0=0, y1=1, yref='paper',
                        line=dict(color='#f74f4f', width=2, dash='dot'))
        fig_m.add_annotation(x=inc_x, y=1, yref='paper', text='incident',
                             showarrow=False, xanchor='left', yanchor='bottom',
                             font=dict(color='#f74f4f', size=11))
        plotly_theme(fig_m)
        st.plotly_chart(fig_m, use_container_width=True)
    else:
        st.markdown(
            "<div class='info-box warn'>No metrics data found for this window.</div>",
            unsafe_allow_html=True
        )

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(
    "<div style='margin-top:3rem;padding-top:1rem;border-top:1px solid #2a2f45;"
    "text-align:center;color:#4a5168;font-size:0.78rem;'>"
    "Log Analytics Platform &nbsp;&middot;&nbsp; CS6P05NM &nbsp;&middot;&nbsp; "
    "<a href='https://github.com/TiraWeb/log-analytics' style='color:#4f8ef7;text-decoration:none;'>GitHub</a>"
    "</div>",
    unsafe_allow_html=True
)

import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# ─── CONFIGURATION & STYLING ───
st.set_page_config(
    page_title="Pipeline Orchestrator",
    page_icon="🎛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Institutional/Charcoal Theme Injection (Fixed Contrast)
st.markdown("""
    <style>
    .main { background-color: #F4F1EA; color: #1A1A1A; }
    
    /* Force metrics boxes to look clean and legible */
    .stMetric { 
        background-color: #FFFFFF !important; 
        padding: 1.5rem !important; 
        border: 1px solid #1A1A1A !important; 
        border-radius: 0px !important; 
        box-shadow: 4px 4px 0px #1A1A1A !important;
    }
    
    /* Ensure metric text, values, and labels are always dark charcoal */
    .stMetric [data-testid="stMetricValue"], 
    .stMetric [data-testid="stMetricLabel"],
    .stMetric [data-testid="stMetricDelta"] { 
        color: #1A1A1A !important; 
    }
    
    .stDataFrame { border: 1px solid #1A1A1A; }
    h1, h2, h3 { font-family: 'Space Grotesk', sans-serif; color: #1A1A1A; font-weight: 700; }
    
    div.stButton > button:first-child {
        background-color: #1A1A1A; color: #F4F1EA; border-radius: 0px; border: 1px solid #1A1A1A;
        font-family: 'JetBrains Mono', monospace; transition: all 0.3s;
    }
    div.stButton > button:first-child:hover { background-color: #2E7D32; border-color: #2E7D32; }
    </style>
""", unsafe_allow_html=True)

# ─── MOCK DATA GENERATION ENGINE (POSTGRESQL METADATA LAYER) ───
@st.cache_data(ttl=60)
def generate_mock_metadata():
    dags = [
        {"DAG ID": "sap_billing_ingest", "Schedule": "0 */2 * * *", "Owner": "FinTech_Data", "Status": "Active"},
        {"DAG ID": "user_behavior_analytics", "Schedule": "30 2 * * *", "Owner": "Growth_AI", "Status": "Active"},
        {"DAG ID": "risk_score_matrix_calc", "Schedule": "@hourly", "Owner": "Risk_Gov", "Status": "Active"},
        {"DAG ID": "legacy_crm_sync", "Schedule": "0 0 * * 0", "Owner": "Data_Ops", "Status": "Paused"},
    ]
    
    # Task instances state simulation
    np.random.seed(42)
    tasks = []
    statuses = ["success", "success", "success", "success", "failed", "running"]
    
    base_time = datetime.now() - timedelta(hours=6)
    for i in range(80):
        dag = np.random.choice([d["DAG ID"] for d in dags])
        state = np.random.choice(statuses, p=[0.7, 0.15, 0.05, 0.05, 0.03, 0.02])
        duration = round(np.random.uniform(12.5, 450.2), 2)
        execution_date = base_time + timedelta(minutes=int(i * 4.5))
        
        tasks.append({
            "Task ID": f"task_node_{i:02d}",
            "DAG ID": dag,
            "Execution Date": execution_date.strftime("%Y-%m-%d %H:%M:%S"),
            "Duration (s)": duration,
            "State": state,
            "Pool": "default_pool",
            "Queue": "celery_workers"
        })
        
    return pd.DataFrame(dags), pd.DataFrame(tasks)

df_dags, df_tasks = generate_mock_metadata()

# ─── APPLICATION SIDEBAR ───
with st.sidebar:
    st.markdown("### 🎛️ SYSTEM CONTROL")
    st.markdown("**Cluster Telemetry Target:**")
    cluster_env = st.selectbox("Environment Target", ["production-core-celery", "staging-k8s-cluster"])
    st.write("---")
    st.markdown("**Engine Live State:**")
    st.success("Airflow Scheduler: ONLINE")
    st.success("PostgreSQL Broker: CONNECTED")
    
    st.write("---")
    st.markdown("### 🛠️ MANUAL WORKFLOW TRIGGER")
    target_dag = st.selectbox("Select Target DAG", df_dags["DAG ID"].tolist())
    if st.button("TRIGGER DAG RUN"):
        st.toast(f"Dispatched Execution Request for {target_dag}!", icon="🚀")

# ─── HEADER LAYER ───
st.title("Pipeline Orchestrator // Real-Time Data Pipeline UI")
st.markdown("Distributed telemetry dashboard tracking ETL tasks scheduling profiles, relational database migrations, and pipeline structural health down to worker-queue execution granularities.")
st.write("---")

# ─── METRIC TELEMETRY ROW ───
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="Total Tracked DAGs", value=len(df_dags))
with col2:
    active_runs = len(df_tasks[df_tasks["State"] == "running"])
    st.metric(label="Active Parallel Tasks", value=active_runs, delta="Live Task Loop" if active_runs > 0 else "Idle")
with col3:
    fail_count = len(df_tasks[df_tasks["State"] == "failed"])
    st.metric(label="Task Failure Incidents (6h)", value=fail_count, delta=f"{fail_count} unresolved", delta_color="inverse")
with col4:
    avg_duration = round(df_tasks["Duration (s)"].mean(), 1)
    st.metric(label="Avg Task Latency", value=f"{avg_duration}s")

st.write("---")

# ─── CORE DASHBOARD SPLIT VIEW ───
layout_col1, layout_col2 = st.columns([3, 2])

with layout_col1:
    st.subheader("📊 Airflow DAG Registry & Active Tasks Stream")
    
    # Filter by state selection tabs
    selected_tab = st.radio("Filter State Registry Logs:", ["All Logged Tasks", "Running Nodes", "Failed Nodes"], horizontal=True)
    
    if selected_tab == "Running Nodes":
        display_tasks = df_tasks[df_tasks["State"] == "running"]
    elif selected_tab == "Failed Nodes":
        display_tasks = df_tasks[df_tasks["State"] == "failed"]
    else:
        display_tasks = df_tasks
        
    st.dataframe(
        display_tasks.sort_values(by="Execution Date", ascending=False),
        use_container_width=True,
        hide_index=True
    )

with layout_col2:
    st.subheader("🚨 Incident Mitigation Engine")
    
    failed_nodes = df_tasks[df_tasks["State"] == "failed"]
    
    if len(failed_nodes) > 0:
        st.error(f"Critical System Interruption: {len(failed_nodes)} Unresolved Task Failures Located.")
        for idx, row in failed_nodes.head(2).iterrows():
            with st.container():
                st.markdown(f"**Target Failure Detected inside `{row['DAG ID']}`**")
                st.caption(f"Node Node Reference: `{row['Task ID']}` // Logged At: {row['Execution Date']}")
                
                # Render simulated raw container traceback text element
                st.code(
                    f"Traceback (most recent call last):\n"
                    f"  File \"/opt/airflow/dags/{row['DAG ID']}.py\", line 42, in execute\n"
                    f"    conn.write(df_payload, target_table='dw_prod_ingress')\n"
                    f"Psycopg2.OperationalError: Connection timed out after 15000ms to PostgreSQL database server.",
                    language="python"
                )
                if st.button("CLEAR LOGS & RE-RUN TASK", key=f"retry_{idx}"):
                    st.toast(f"Sent retry payload structural instruction to Celery node worker for {row['Task ID']}!", icon="🔄")
    else:
        st.success("System Verification Cleared: No active execution trace exceptions inside monitoring limits.")

st.write("---")

# ─── UNDERLYING PERSISTENCE SIMULATOR (POSTGRESQL METADATA DRILLDOWN) ───
st.subheader("🗄️ Relational Metadata Schema Search Layer (PostgreSQL Raw Layer)")
st.markdown("Perform index querying directly against the live database metadata ledger records mapping execution metrics history.")

search_query = st.text_input("Database Query Index (Filter by DAG or Task Parameter Strings):", placeholder="e.g., sap_billing_ingest")

if search_query:
    filtered_results = df_tasks[
        df_tasks["DAG ID"].str.contains(search_query, case=False) | 
        df_tasks["Task ID"].str.contains(search_query, case=False)
    ]
    st.write(f"Query returned {len(filtered_results)} matches inside raw relational cache indexes:")
    st.dataframe(filtered_results, use_container_width=True, hide_index=True)
else:
    st.info("Provide query string vectors above to inspect low-level table entries mapped directly from PostgreSQL schemas.")

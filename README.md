#  Pipeline Orchestrator: Real-Time Distributed ETL UI & Alert Engine

[!(https://Pipeline-Orchestrator.streamlit.app/)]

Let’s be honest: tracking distributed ETL data pipelines at scale can quickly turn into an absolute nightmare. When a data migration fails or a database connection times out at 3:00 AM, digging through millions of lines of raw server text logs is the last thing you want to do.

I built this **Pipeline Orchestrator UI** to act as a mission-control dashboard for complex backend data pipelines. It pulls operational logs and transactional metadata out of a database layer and organizes them into a clean, actionable visual control room. It bridges raw data architecture with real-world infrastructure monitoring.

---

##  What This App Does

* ** Live Telemetry At A Glance:** Instantly tracks total scheduled workflows (DAGs), actively running processes, overall system latency, and critical failure incidents.
* ** Incident Mitigation Panel:** Instead of hiding error messages, this dashboard actively catches failed task instances, surfaces raw python tracebacks (like connection timeouts), and provides a working **"Clear Logs & Re-Run Task"** button to simulate fixing server node issues on the fly.
* ** Relational Ledger Deep Search:** Includes a dynamic search bar that queries metadata histories directly by pipeline or task parameters, mimicking index lookups against a live PostgreSQL schema cache.
* ** Manual Workflow Dispatches:** Features a sidebar engine controls deck allowing system administrators to manually select any data workflow and trigger an immediate run.

---

##  The Tech Stack Inside

* **Operational Interface:** Built with `Streamlit`, customized with a brutalist, high-contrast institutional theme designed to prioritize glanceable data density.
* **Orchestration Architecture:** Designed around `Apache Airflow` DAG scheduling patterns and execution state workflows.
* **Metadata & Broker Layer:** Mimics a relational `PostgreSQL` ledger system tracking dynamic task states, execution pool limits, and worker distribution metrics.

---

##  Running It Locally

If you want to pull this code down and run the control center on your machine, follow these quick steps:

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/haz4rl/Pipeline-Orchestrator.git](https://github.com/haz4rl/Pipeline-Orchestrator.git)
   cd Pipeline-Orchestrator
2. **Install the dependencies
   ```bash
   Make sure you have your virtual environment set up (Python 3.11+ recommended):
   pip install streamlit pandas numpy 

3. Fire up the engine:
   streamlit run app.py

# Superstore ETL Pipeline — Automated, Containerised, Production-Ready

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apache-airflow)
![Docker](https://img.shields.io/badge/Docker-24.0.5-2496ED?logo=docker)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?logo=postgresql)

## Project Overview
**A fully automated data pipeline that transforms raw sales data into actionable business insights — running daily, error‑proof, and one command away.**

## The Problem
The analytics team spent 5 hours each week manually downloading CSVs and running Python scripts. And they also encountered difficulty in maintaining scripts and scheduling it. This project automates that process.

---

## 💼 Business Value

In a real‑world analytics team, manually downloading CSVs, cleaning data, and re‑running reports is time‑consuming and error‑prone. This project eliminates that manual effort **by 100%**:

- **Daily, unattended execution** – the pipeline runs at midnight without human intervention, ensuring reports are always up‑to‑date.
- **Built‑in data quality checks** – every load is verified for row counts and revenue totals; mismatches stop the pipeline immediately, preventing dirty data from reaching decision‑makers.
- **Fully reproducible** – the whole stack (orchestrator, database, dependencies) is defined as code and starts with a single `docker compose up`, making it portable across any machine or cloud environment.
- **Production‑grade monitoring** – task‑level logs, retries on failure, and a clear DAG topology give full observability into the health of the data flow.

---

## 🏗️ Architecture
CSV → Python (Pandas) → PostgreSQL → Airflow DAG (daily)


All services (Airflow webserver, scheduler, worker, Redis, target PostgreSQL) run inside Docker containers, orchestrated via `docker‑compose`. The DAG:

1. **Extracts** the CSV from a public URL (idempotent, caches locally).
2. **Transforms** the data with pandas (date parsing, deduplication, derived columns).
3. **Loads** into a dedicated PostgreSQL instance, using `NUMERIC` types for financial accuracy.
4. **Verifies** total sales and row counts between the transformed data and the database.
5. **Generates** a business‑ready summary report: top categories, top 10 customers, monthly trends.

---

## 🧱 Tech Stack & Why

| Technology | Role | Business Rationale |
|------------|------|-------------------|
| **Apache Airflow** | Orchestration & Scheduling | Industry standard for batch pipelines; DAGs are code, making version control and collaboration easy. |
| **Docker Compose** | Infrastructure as Code | Guarantees identical behaviour on any machine; eliminates “works on my machine” issues. |
| **pandas** | Data Transformation | Fast, readable, and ubiquitous in data engineering; performs complex cleaning with minimal code. |
| **PostgreSQL** | Target Database | ACID‑compliant, reliable; `NUMERIC` type avoids floating‑point rounding errors in financial sums. |
| **SQLAlchemy** | Database Interaction | Abstracts database connections, allowing the same Python code to work with different DB backends. |
| **Verification Step** | Data Quality | Catches loading errors immediately, ensuring the report reflects accurate numbers. |

---

## 🚀 How to Run (on any machine with Docker)

# 1. Clone the repository
git clone https://github.com/your-username/airflow-superstore-etl.git
cd airflow-superstore-etl

# 2. Set environment variables (adjust UID for your system)

echo "AIRFLOW_UID=$(id -u)" > .env

echo "AIRFLOW_GID=0" >> .env

echo "POSTGRES_PASSWORD=airflow" >> .env

echo "AIRFLOW__CORE__LOAD_EXAMPLES=false" >> .env


# 3. Start everything
docker compose up -d
Open http://localhost:8080 (airflow / airflow). 

The DAG superstore_etl is loaded automatically. 

Toggle it on to start the daily schedule, or trigger a manual run. 

After a successful run, check ./data/summary.txt for the report.

To stop: docker compose down (add -v to also wipe the database volume).

📊 Report Sample
```
=== Superstore ETL Summary Report ===

Generated: 2026-07-04 08:00:00
--- Sales by Category ---
  Technology: $836,154.03
  Furniture: $742,000.18
  Office Supplies: $719,046.65

--- Top 10 Customers ---
  Sean Miller: $25,043.05
  Tamara Chand: $19,005.42

--- Latest 6 Months ---
  2017-12: $245,639.21
  2017-11: $213,542.87

```
✅ Data Quality & Fault Tolerance
Idempotency: The table is truncated before each load, so re‑running the DAG produces identical results.
Automatic retries: Each task retries twice on failure with a 5‑minute delay — handling transient network or database issues gracefully.
Verification gate: If row counts or revenue totals differ by more than $0.05 between the cleaned data and the database, the pipeline fails explicitly, preventing downstream consumers from using bad data.
Atomic load: The truncate‑and‑insert pattern ensures no partial data is exposed to end users.


🔮 Extensibility
This project is built as a template for any CSV‑to‑PostgreSQL ETL. To adapt it for a real business:
Replace the CSV URL and column mappings in dags/superstore_etl.py.
Swap the target database to a cloud instance (AWS RDS, GCP Cloud SQL) by changing the connection string.
Add email alerts via Mailtrap or a real SMTP server (configurable in docker-compose.yaml and Airflow connections).
Orchestrate multiple DAGs (e.g., separate pipelines for inventory, finance) with the same Airflow instance.


📬 About Me
Data engineer passionate about building reliable, automated data infrastructure that turns raw information into business value.
Let’s connect on LinkedIn[https://www.linkedin.com/in/jan-wharry-lloyd-yap-3724b3337/] or explore more on GitHub[https://github.com/wharryG].

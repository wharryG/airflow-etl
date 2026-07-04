# Superstore ETL Pipeline with Airflow & Docker

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-blue)
![Docker](https://img.shields.io/badge/Docker-24.0.5-blue)
![Python](https://img.shields.io/badge/Python-3.9+-blue)

A fully containerised data pipeline that extracts the **Global Superstore** dataset, cleans it, loads it into a PostgreSQL database, and generates a business summary report. Orchestrated by **Apache Airflow** and packaged with **Docker Compose**.

---

## 📁 Folder Structure
airflow-superstore-etl/
├── dags/
│ └── superstore_etl.py # Airflow DAG definition + tasks
├── data/ # Mounted folder for CSV, pickle, and reports
├── logs/ # Airflow logs (gitignored)
├── plugins/ # Custom Airflow plugins (if any)
├── config/ # Airflow configuration overrides
├── docker-compose.yaml # Services: Airflow, Postgres, Redis, superstore_db
├── .env # Environment variables
├── .gitignore # Ignores generated files
└── README.md # This file


---

## 🚀 Quick Start

### Prerequisites

- **Docker** and **Docker Compose** installed.
- Git (to clone the repository).

### 1. Clone the Repository

```bash
git clone https://github.com/[your-username]/airflow-superstore-etl.git
cd airflow-superstore-etl

echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env
echo "POSTGRES_PASSWORD=airflow" >> .env
echo "AIRFLOW__CORE__LOAD_EXAMPLES=false" >> .env
```

### Start Services
docker compose up -d


### Access Airflow
Open your browser at http://localhost:8080

Username: airflow

Password: airflow

You’ll see the superstore_etl DAG. Toggle the switch to activate it.


⚙️ Pipeline Overview
The DAG runs daily at midnight and consists of six tasks:

Task	Description
create_table_if_needed	Creates the superstore_orders table in the target PostgreSQL database (if it doesn’t exist).
extract_csv	Downloads the Superstore CSV from IBM’s repository (or uses a cached local copy).
transform_data	Cleans data with pandas: converts dates, standardises strings, adds derived columns (is_loss, delivery_days), drops duplicates, and saves a pickle.
load_data	Truncates the target table and loads the cleaned DataFrame.
verify_data	Compares row counts and total sales between the pickle and the database to ensure consistency.
generate_report	Queries the database for top categories, customers, and monthly sales, then writes summary.txt to the data/ folder.
The report is accessible on your host machine at ./data/summary.txt after a successful run.

🗄️ Target Database
The pipeline loads data into a separate PostgreSQL container named superstore_db.

Host: superstore_db (accessible from Airflow containers via Docker network)

Port: 5432 (mapped to 5433 on your host)

Database: superstore

User / Password: postgres / zxc

You can connect to it manually from your host:
psql -h localhost -p 5433 -U postgres -d superstore

🛑 Stopping the Pipeline
docker compose down
docker compose down -v

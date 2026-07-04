"""
Superstore ETL DAG
Extracts a CSV, cleans it, loads into a PostgreSQL database,
verifies the data, and generates a report.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

import os
import pandas as pd
from sqlalchemy import create_engine, text
import requests

# ---------- CONFIG ----------
CSV_URL = "https://raw.githubusercontent.com/IBM/superstore/master/superstore.csv"
DATA_DIR = "/opt/airflow/data"
LOCAL_CSV_PATH = os.path.join(DATA_DIR, "superstore.csv")
CLEANED_PICKLE_PATH = os.path.join(DATA_DIR, "cleaned_superstore.pkl")
REPORT_PATH = os.path.join(DATA_DIR, "summary.txt")

# Target database connection (from the superstore_db container)
DB_USER = "postgres"
DB_PASSWORD = "zxc"
DB_HOST = "superstore_db"   # Docker service name = hostname
DB_PORT = "5432"
DB_NAME = "superstore"

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------- TASK FUNCTIONS ----------

def create_table_if_needed():
    """Ensure the superstore_orders table exists."""
    engine = create_engine(ENGINE_URL)
    create_sql = """
    CREATE TABLE IF NOT EXISTS superstore_orders (
        order_item_id SERIAL PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        order_date DATE NOT NULL,
        ship_date DATE,
        ship_mode VARCHAR(50),
        customer_id VARCHAR(50),
        customer_name VARCHAR(100),
        segment VARCHAR(50),
        country VARCHAR(50),
        city VARCHAR(100),
        state VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(50),
        product_id VARCHAR(50) NOT NULL,
        category VARCHAR(50),
        sub_category VARCHAR(50),
        product_name VARCHAR(255),
        sales NUMERIC(10,2),
        quantity INTEGER,
        discount NUMERIC(3,2),
        profit NUMERIC(10,2),
        is_loss BOOLEAN,
        delivery_days INTEGER,
        UNIQUE(order_id, product_id)
    );
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(create_sql))
    print("Table 'superstore_orders' is ready.")


def extract_csv():
    """Download the CSV file if it's not already present, with error handling."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Remove stale empty file if present
    if os.path.exists(LOCAL_CSV_PATH) and os.path.getsize(LOCAL_CSV_PATH) == 0:
        os.remove(LOCAL_CSV_PATH)

    if not os.path.exists(LOCAL_CSV_PATH):
        print("Downloading CSV...")
        r = requests.get(CSV_URL)
        r.raise_for_status()          # Raise an exception for HTTP errors
        with open(LOCAL_CSV_PATH, "wb") as f:
            f.write(r.content)
        # Verify file is not empty
        if os.path.getsize(LOCAL_CSV_PATH) == 0:
            raise AirflowFailException("Downloaded CSV is empty.")
        print("Download complete.")
    else:
        print("CSV already exists.")


def transform_data():
    """Clean the CSV and save a pickled DataFrame."""
    df = pd.read_csv(LOCAL_CSV_PATH, encoding='ISO-8859-1')
    print(f"Raw rows: {df.shape[0]}")

    # Convert dates
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])

    # Postal code as string
    df['Postal Code'] = df['Postal Code'].astype(str)

    # Clean strings
    df['Customer Name'] = df['Customer Name'].str.strip().str.title()
    df['Category'] = df['Category'].str.strip().str.title()

    # Add derived columns
    df['is_loss'] = df['Profit'] < 0
    df['Delivery Days'] = (df['Ship Date'] - df['Order Date']).dt.days

    # Rename columns
    column_mapping = {
        'Row ID': 'order_item_id',
        'Order ID': 'order_id',
        'Order Date': 'order_date',
        'Ship Date': 'ship_date',
        'Ship Mode': 'ship_mode',
        'Customer ID': 'customer_id',
        'Customer Name': 'customer_name',
        'Segment': 'segment',
        'Country': 'country',
        'City': 'city',
        'State': 'state',
        'Postal Code': 'postal_code',
        'Region': 'region',
        'Product ID': 'product_id',
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Product Name': 'product_name',
        'Sales': 'sales',
        'Quantity': 'quantity',
        'Discount': 'discount',
        'Profit': 'profit',
        'is_loss': 'is_loss',
        'Delivery Days': 'delivery_days'
    }
    df.rename(columns=column_mapping, inplace=True)

    # Drop auto-generated ID (we have DB serial)
    if 'order_item_id' in df.columns:
        df.drop('order_item_id', axis=1, inplace=True)

    # Round money to match NUMERIC(10,2)
    df['sales'] = df['sales'].round(2)
    df['profit'] = df['profit'].round(2)

    # Remove duplicates on (order_id, product_id)
    before = len(df)
    df.drop_duplicates(subset=['order_id', 'product_id'], inplace=True)
    after = len(df)
    print(f"Dropped {before - after} duplicate rows.")

    # Save cleaned pickle
    df.to_pickle(CLEANED_PICKLE_PATH)
    print(f"Cleaned data saved to {CLEANED_PICKLE_PATH} ({after} rows)")


def load_data():
    """Truncate target table and load the cleaned DataFrame."""
    df = pd.read_pickle(CLEANED_PICKLE_PATH)
    engine = create_engine(ENGINE_URL)

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("TRUNCATE TABLE superstore_orders RESTART IDENTITY CASCADE"))
    print("Table truncated.")

    df.to_sql('superstore_orders', engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows into database.")


def verify_data():
    """Compare row count and total sales between pickle and database."""
    df = pd.read_pickle(CLEANED_PICKLE_PATH)
    engine = create_engine(ENGINE_URL)

    pandas_sales = df['sales'].sum()
    pandas_rows = len(df)

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*), COALESCE(SUM(sales), 0) FROM superstore_orders"
        )).fetchone()
        db_rows, db_sales = int(result[0]), float(result[1])

    print(f"Pandas: {pandas_rows} rows, ${pandas_sales:,.2f}")
    print(f"DB:     {db_rows} rows, ${db_sales:,.2f}")

    if pandas_rows != db_rows:
        raise AirflowFailException(f"Row count mismatch: pandas={pandas_rows}, db={db_rows}")
    if abs(pandas_sales - db_sales) > 0.05:
        raise AirflowFailException(f"Sales total mismatch: pandas={pandas_sales}, db={db_sales}")


def generate_report():
    """Query the database and write a summary text file."""
    engine = create_engine(ENGINE_URL)
    with engine.connect() as conn:
        cat_rows = conn.execute(text("""
            SELECT category, ROUND(SUM(sales)::numeric, 2) AS total_sales
            FROM superstore_orders
            GROUP BY category ORDER BY total_sales DESC
        """)).fetchall()
        top_cust = conn.execute(text("""
            SELECT customer_name, ROUND(SUM(sales)::numeric, 2) AS total_revenue
            FROM superstore_orders
            GROUP BY customer_name ORDER BY total_revenue DESC LIMIT 10
        """)).fetchall()
        monthly = conn.execute(text("""
            SELECT TO_CHAR(order_date, 'YYYY-MM') AS month,
                   ROUND(SUM(sales)::numeric, 2) AS total_sales
            FROM superstore_orders
            GROUP BY month ORDER BY month DESC LIMIT 6
        """)).fetchall()

    lines = ["=== Superstore ETL Summary Report ==="]
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("--- Sales by Category ---")
    for name, amount in cat_rows:
        lines.append(f"  {name}: ${amount:,.2f}")
    lines.append("--- Top 10 Customers ---")
    for name, amount in top_cust:
        lines.append(f"  {name}: ${amount:,.2f}")
    lines.append("--- Latest 6 Months ---")
    for month, amount in monthly:
        lines.append(f"  {month}: ${amount:,.2f}")

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"Report saved to {REPORT_PATH}")


# ---------- DAG DEFINITION ----------

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,           # Disabled until SMTP is configured
    'email': ['you@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'superstore_etl',
    default_args=default_args,
    description='ETL pipeline for Superstore dataset',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['superstore', 'etl'],
)

# ---------- TASKS ----------

t_create_table = PythonOperator(
    task_id='create_table_if_needed',
    python_callable=create_table_if_needed,
    dag=dag,
)

t_extract = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag,
)

t_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t_verify = PythonOperator(
    task_id='verify_data',
    python_callable=verify_data,
    dag=dag,
)

t_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# ---------- TASK DEPENDENCIES ----------
t_create_table >> t_load
t_extract >> t_transform >> t_load >> t_verify >> t_report
"""
dags/nammamart_elt_rudin.py
--------------------------
NammaMart ELT pipeline.

Flow:
  CSV exports -> raw DuckDB tables -> SQL staging -> SQL marts

Raw tables retain every source value as VARCHAR together with load metadata.
All cleaning, casting, masking, deduplication, and quality status logic is
kept in SQL files under include/sql/.
"""

import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.utils.context import get_current_context


DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_DIR = "/usr/local/airflow/include/sql"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rudin.duckdb"

default_args = {
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _execute_sql(con, filename: str, run_id: str = "", dag_id: str = "") -> None:
    """Read a version-controlled SQL transformation and execute it in DuckDB."""
    with open(os.path.join(SQL_DIR, filename), encoding="utf-8") as sql_file:
        statement = sql_file.read().replace("{{ run_id }}", run_id).replace("{{ dag_id }}", dag_id)
    con.execute(statement)


def _load_csv_to_raw(filename: str, table_name: str) -> int:
    """Append an untouched CSV snapshot to a raw table with audit metadata."""
    context = get_current_context()
    run_id = context["run_id"]
    frame = pd.read_csv(f"{DATA_DIR}/{filename}", dtype=str, keep_default_na=False)
    frame["_loaded_at"] = pendulum.now("UTC").to_iso8601_string()
    frame["_source_file"] = filename
    frame["_run_id"] = run_id

    con = duckdb.connect(WAREHOUSE_DB)
    con.register("raw_source_df", frame)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM raw_source_df WHERE 1=0")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM raw_source_df")
    con.close()
    print(f"[Raw] {filename}: appended {len(frame)} rows to {table_name}")
    return len(frame)


@dag(
    dag_id="nammamart_elt_rudin",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt_rudin():
    # -------------------- RAW: append every source record --------------------
    @task
    def load_raw_orders():
        return _load_csv_to_raw("orders.csv", "raw_orders")

    @task
    def load_raw_payments():
        return _load_csv_to_raw("payments.csv", "raw_payments")

    @task
    def load_raw_customers():
        return _load_csv_to_raw("customers.csv", "raw_customers")

    @task
    def load_raw_products():
        return _load_csv_to_raw("products.csv", "raw_products")

    @task
    def load_raw_stores():
        return _load_csv_to_raw("stores.csv", "raw_stores")

    @task
    def load_raw_increment():
        return _load_csv_to_raw("orders_2026-10-01_increment.csv", "raw_orders")

    # -------------------- STAGING: SQL-only transformations ------------------
    @task
    def build_staging(*raw_loads):
        """Run all staging casts, standardization, deduplication, and DQ logic."""
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_staging.sql", context["run_id"], context["dag"].dag_id)
        con.close()
        print(f"[Staging] SQL transformations complete for {len(raw_loads)} raw loads")
        return True

    # -------------------- DQ: persist SQL quality scorecard ------------------
    @task
    def record_dq_results(staging_complete):
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_dq_results.sql", context["run_id"], context["dag"].dag_id)
        print(con.execute("SELECT * FROM dq_results WHERE run_id = ?", [context["run_id"]]).fetchdf().to_string(index=False))
        con.close()
        return staging_complete

    # -------------------- MART: SQL-only business tables ---------------------
    @task
    def build_marts(dq_complete):
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_marts.sql", context["run_id"], context["dag"].dag_id)
        for table_name in ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla", "mart_payment_reconciliation", "mart_top_customers"]:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[Mart] {table_name}: {row_count} rows")
        con.close()

    raw_orders = load_raw_orders()
    raw_payments = load_raw_payments()
    raw_customers = load_raw_customers()
    raw_products = load_raw_products()
    raw_stores = load_raw_stores()
    raw_increment = load_raw_increment()

    # The increment is loaded after the base orders so staging's latest-record
    # window function applies the correction rows over the September snapshot.
    raw_orders >> raw_increment
    staging = build_staging(raw_orders, raw_increment, raw_payments, raw_customers, raw_products, raw_stores)
    dq = record_dq_results(staging)
    build_marts(dq)


nammamart_elt_rudin()

"""
dags/namma_etl_rudin.py
-----------------------
NammaMart ETL pipeline.

Flow:
  five parallel extracts -> Python validation -> quarantine/transform
  -> idempotent DuckDB star-schema load -> reconciliation and reports

The pipeline reads the untouched CSV exports from include/data/nammamart,
keeps failed rows outside the finance tables, and writes the personal
warehouse to include/warehouse_rudin.duckdb.
"""

import json
import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.utils.context import get_current_context


DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rudin.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/rudin"

ALLOWED_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}

default_args = {
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _read_json(value: str) -> pd.DataFrame:
    """Read a DataFrame serialized by an upstream TaskFlow task."""
    return pd.read_json(value, orient="records")


def _json_records(frame: pd.DataFrame) -> str:
    """Serialize a DataFrame without converting its columns into index keys."""
    return frame.to_json(orient="records", date_format="iso")


def _blank(value) -> bool:
    """Return whether a source value is missing or contains only whitespace."""
    return pd.isna(value) or str(value).strip() == ""


def _add_failure(failures, index, reason: str) -> None:
    """Add a failure reason to a row while preserving multiple failed rules."""
    failures.setdefault(index, []).append(reason)


def _mask_email(value: str) -> str:
    """Keep enough email structure for support while hiding the local part."""
    if _blank(value) or "@" not in str(value):
        return "***"
    local, domain = str(value).split("@", 1)
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: str) -> str:
    """Keep only the last two phone digits for masked reporting tables."""
    value = str(value)
    return f"{'*' * max(len(value) - 2, 0)}{value[-2:]}" if value else "***"


@dag(
    dag_id="nammamart_etl_rudin",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "etl"],
)
def nammamart_etl_rudin():
    # -------------------- EXTRACT: five independent tasks --------------------
    # Each task returns its own JSON payload, so Airflow can run all five
    # source reads in parallel and pass the results through XCom.
    @task
    def extract_orders():
        frame = pd.read_csv(f"{DATA_DIR}/orders.csv", dtype=str, keep_default_na=False)
        print(f"[Extract] orders.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_payments():
        frame = pd.read_csv(f"{DATA_DIR}/payments.csv", dtype=str, keep_default_na=False)
        print(f"[Extract] payments.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_customers():
        frame = pd.read_csv(f"{DATA_DIR}/customers.csv", dtype=str, keep_default_na=False)
        print(f"[Extract] customers.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_products():
        frame = pd.read_csv(f"{DATA_DIR}/products.csv", dtype=str, keep_default_na=False)
        print(f"[Extract] products.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_stores():
        frame = pd.read_csv(f"{DATA_DIR}/stores.csv", dtype=str, keep_default_na=False)
        print(f"[Extract] stores.csv: {len(frame)} rows")
        return _json_records(frame)

    # -------------------- VALIDATE: Python data-quality rules ---------------
    @task
    def validate(orders_json, payments_json, customers_json, products_json, stores_json):
        """Split each source into passed/failed rows and retain failure reasons."""
        orders = _read_json(orders_json)
        payments = _read_json(payments_json)
        customers = _read_json(customers_json)
        products = _read_json(products_json)
        stores = _read_json(stores_json)
        frames = {"orders": orders, "payments": payments, "customers": customers, "products": products, "stores": stores}
        failures = {name: {} for name in frames}

        # Master keys support the cross-file referential-integrity checks.
        customer_ids = set(customers["customer_id"])
        product_ids = set(products["product_id"])
        store_ids = set(stores["store_id"])
        order_ids = set(orders["order_id"])

        # Primary-key, completeness, and uniqueness checks for every source.
        key_columns = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id", "products": "product_id", "stores": "store_id"}
        for name, frame in frames.items():
            key = key_columns[name]
            for index, value in frame[key].items():
                if _blank(value):
                    _add_failure(failures[name], index, f"{key} is blank")
            duplicate_rows = frame[key].duplicated(keep=False)
            for index in frame.index[duplicate_rows]:
                _add_failure(failures[name], index, f"duplicate {key}")

        # Orders: required keys, allowed values, arithmetic, timestamps, and SLA.
        for index, row in orders.iterrows():
            if _blank(row.customer_id) or row.customer_id not in customer_ids:
                _add_failure(failures["orders"], index, "customer_id is missing or unknown")
            if _blank(row.product_id) or row.product_id not in product_ids:
                _add_failure(failures["orders"], index, "product_id is missing or unknown")
            if _blank(row.store_id) or row.store_id not in store_ids:
                _add_failure(failures["orders"], index, "store_id is missing or unknown")
            status = str(row.order_status).strip().upper()
            mode = str(row.payment_mode).strip()
            if status not in ALLOWED_STATUS:
                _add_failure(failures["orders"], index, f"invalid order_status: {row.order_status}")
            if mode not in ALLOWED_PAYMENT_MODES:
                _add_failure(failures["orders"], index, f"invalid payment_mode: {row.payment_mode}")
            quantity, unit_price, discount, amount = [pd.to_numeric(row[column], errors="coerce") for column in ["quantity", "unit_price", "discount_pct", "order_amount"]]
            if pd.isna(quantity) or quantity <= 0:
                _add_failure(failures["orders"], index, "quantity must be greater than zero")
            if pd.notna(discount) and not 0 <= discount <= 100:
                _add_failure(failures["orders"], index, "discount_pct must be between 0 and 100")
            if all(pd.notna(value) for value in [quantity, unit_price, discount, amount]):
                expected = round(quantity * unit_price * (1 - discount / 100), 2)
                if abs(amount - expected) > 0.01:
                    _add_failure(failures["orders"], index, f"order_amount does not match calculated value {expected}")
            order_ts = pd.to_datetime(row.order_ts, errors="coerce")
            if pd.isna(order_ts):
                _add_failure(failures["orders"], index, "order_ts cannot be parsed")
            elif order_ts > pd.Timestamp("2026-10-09 23:59:59"):
                _add_failure(failures["orders"], index, "order_ts is future dated")
            delivery = pd.to_numeric(row.delivery_minutes, errors="coerce")
            if status == "DELIVERED" and pd.isna(delivery):
                _add_failure(failures["orders"], index, "DELIVERED order has no delivery_minutes")
            if pd.notna(delivery) and delivery >= 120:
                _add_failure(failures["orders"], index, "delivery_minutes must be below 120")

        # Payments: validate keys, allowed values, timestamps, and reconciliation.
        order_amounts = orders.groupby("order_id")["order_amount"].first()
        for index, row in payments.iterrows():
            if str(row.payment_mode).strip() not in ALLOWED_PAYMENT_MODES:
                _add_failure(failures["payments"], index, f"invalid payment_mode: {row.payment_mode}")
            if row.order_id not in order_ids:
                _add_failure(failures["payments"], index, "payment references unknown order")
            if pd.isna(pd.to_datetime(row.payment_ts, errors="coerce")):
                _add_failure(failures["payments"], index, "payment_ts cannot be parsed")
            if str(row.payment_status).strip().upper() == "SUCCESS":
                paid = pd.to_numeric(row.amount_paid, errors="coerce")
                expected = pd.to_numeric(order_amounts.get(row.order_id), errors="coerce")
                if pd.notna(paid) and pd.notna(expected) and abs(paid - expected) > 0.01:
                    _add_failure(failures["payments"], index, "SUCCESS amount_paid does not equal order_amount")

        # Customer, product, and store checks. PII format failures are warnings
        # in this DAG and do not prevent a row from loading into masked dimensions.
        for index, row in customers.iterrows():
            if not str(row.email).strip() or "@" not in str(row.email):
                print(f"[Validate][Warning] customer {row.customer_id} has an invalid email")
            if not str(row.phone).isdigit() or len(str(row.phone)) != 10:
                print(f"[Validate][Warning] customer {row.customer_id} has an invalid phone")
            signup_date = pd.to_datetime(row.signup_date, errors="coerce")
            if pd.isna(signup_date) or signup_date > pd.Timestamp("2026-10-09"):
                _add_failure(failures["customers"], index, "signup_date is invalid or future dated")
        for index, row in products.iterrows():
            mrp = pd.to_numeric(row.mrp, errors="coerce")
            cost_price = pd.to_numeric(row.cost_price, errors="coerce")
            if _blank(row.category):
                _add_failure(failures["products"], index, "category is blank")
            if pd.isna(mrp) or mrp <= 0:
                _add_failure(failures["products"], index, "mrp must be positive")
            if pd.notna(mrp) and pd.notna(cost_price) and cost_price > mrp:
                _add_failure(failures["products"], index, "cost_price exceeds mrp")
        for index, row in stores.iterrows():
            if _blank(row.zone):
                _add_failure(failures["stores"], index, "zone is blank")

        # Return a compact XCom payload containing both clean and failed rows.
        result = {}
        for name, frame in frames.items():
            failed_indexes = list(failures[name])
            failed = frame.loc[failed_indexes].copy() if failed_indexes else frame.iloc[0:0].copy()
            failed["failure_reason"] = ["; ".join(failures[name][index]) for index in failed.index]
            passed = frame.drop(index=failed_indexes).copy()
            result[name] = {"passed": _json_records(passed), "failed": _json_records(failed), "passed_count": len(passed), "failed_count": len(failed)}
            print(f"[Validate] {name}: passed={len(passed)} failed={len(failed)}")
        return result

    # -------------------- QUARANTINE: preserve failed rows ------------------
    @task
    def quarantine(validation_result):
        """Write every failed row to a run-specific CSV with its reason."""
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        total_failed = 0
        for name, result in validation_result.items():
            failed = _read_json(result["failed"])
            if failed.empty:
                continue
            path = f"{QUARANTINE_DIR}/{name}_failed_{run_id}.csv"
            failed.to_csv(path, index=False)
            total_failed += len(failed)
            print(f"[Quarantine] {len(failed)} rows written to {path}")
        return total_failed

    # -------------------- TRANSFORM: standardize and derive metrics ----------
    @task
    def transform(validation_result):
        """Clean passed rows, mask dimensions, join them, and derive metrics."""
        orders = _read_json(validation_result["orders"]["passed"])
        customers = _read_json(validation_result["customers"]["passed"])
        products = _read_json(validation_result["products"]["passed"])
        stores = _read_json(validation_result["stores"]["passed"])

        # Normalize whitespace and known casing variants before joining.
        for frame in [orders, customers, products, stores]:
            for column in frame.select_dtypes(include="object").columns:
                frame[column] = frame[column].str.strip()
        orders["order_status"] = orders["order_status"].str.upper()
        orders["payment_mode"] = orders["payment_mode"].replace({"upi": "UPI", "UPI ": "UPI", "cod": "Cash on Delivery"})
        customers["loyalty_tier"] = customers["loyalty_tier"].str.title()

        # Deduplicate dimensions and mask PII before they enter reporting tables.
        customers = customers.drop_duplicates("customer_id", keep="last").copy()
        products = products.drop_duplicates("product_id", keep="last").copy()
        stores = stores.drop_duplicates("store_id", keep="last").copy()
        customers["email"] = customers["email"].map(_mask_email)
        customers["phone"] = customers["phone"].map(_mask_phone)

        orders["quantity"] = pd.to_numeric(orders["quantity"])
        orders["unit_price"] = pd.to_numeric(orders["unit_price"])
        orders["discount_pct"] = pd.to_numeric(orders["discount_pct"])
        orders["order_amount"] = pd.to_numeric(orders["order_amount"])
        orders["delivery_minutes"] = pd.to_numeric(orders["delivery_minutes"])
        products["cost_price"] = pd.to_numeric(products["cost_price"])
        orders["order_ts"] = pd.to_datetime(orders["order_ts"])
        orders["order_date"] = orders["order_ts"].dt.date.astype(str)
        orders["net_revenue"] = orders["order_amount"].where(orders["order_status"] == "DELIVERED", 0.0)
        orders["within_15_min"] = (orders["order_status"] == "DELIVERED") & (orders["delivery_minutes"] <= 15)

        # Join the clean fact rows to product cost and store/customer attributes.
        fact = orders.merge(products[["product_id", "cost_price"]], on="product_id", how="left")
        fact = fact.merge(customers[["customer_id"]], on="customer_id", how="inner")
        fact = fact.merge(stores[["store_id"]], on="store_id", how="inner")
        fact["gross_margin"] = fact["net_revenue"] - (fact["quantity"] * fact["cost_price"])
        return {"fact": _json_records(fact), "customers": _json_records(customers), "products": _json_records(products), "stores": _json_records(stores)}

    # -------------------- LOAD: idempotent DuckDB star schema ----------------
    @task
    def load_star_schema(transformed):
        """Replace the current transformed snapshot so retries never double-count."""
        fact = _read_json(transformed["fact"])
        customers = _read_json(transformed["customers"])
        products = _read_json(transformed["products"])
        stores = _read_json(transformed["stores"])
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("fact_df", fact)
        con.register("customer_df", customers)
        con.register("product_df", products)
        con.register("store_df", stores)
        con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM fact_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM customer_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM product_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM store_df")
        print(f"[Load] etl_fact_orders={len(fact)} rows; dimensions refreshed idempotently")
        con.close()

    # -------------------- RECONCILE: order/payment mismatches ----------------
    @task
    def reconcile_payments(validation_result):
        """Write one row for every payment/order mismatch, including missing sides."""
        orders = _read_json(validation_result["orders"]["passed"])
        payments = _read_json(validation_result["payments"]["passed"])
        orders["order_amount"] = pd.to_numeric(orders["order_amount"], errors="coerce")
        payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce")
        successful = payments[payments["payment_status"].str.upper() == "SUCCESS"]
        comparison = successful.merge(orders[["order_id", "order_amount"]], on="order_id", how="outer", indicator=True)
        comparison["difference"] = comparison["amount_paid"] - comparison["order_amount"]
        mismatch = comparison[(comparison["_merge"] != "both") | (comparison["difference"].abs() > 0.01)].copy()
        mismatch["reason"] = mismatch.apply(lambda row: "missing order" if row["_merge"] == "left_only" else "missing successful payment" if row["_merge"] == "right_only" else "amount mismatch", axis=1)
        mismatch = mismatch.drop(columns=["_merge"])
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("reconciliation_df", mismatch)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM reconciliation_df")
        con.close()
        print(f"[Reconcile] {len(mismatch)} mismatches written")

    # -------------------- REPORT: business-ready summary tables --------------
    @task
    def report():
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_id, SUM(net_revenue) AS net_revenue
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, zone, store_id
            ORDER BY order_date, zone, store_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, SUM(f.net_revenue) AS revenue, SUM(f.gross_margin) AS gross_margin
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY p.category
        """)
        print(con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 10").fetchdf().to_string(index=False))
        print(con.execute("SELECT * FROM etl_category_margin").fetchdf().to_string(index=False))
        con.close()

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    validated = validate(orders, payments, customers, products, stores)
    quarantine_task = quarantine(validated)
    transformed = transform(validated)
    loaded = load_star_schema(transformed)
    reconciled = reconcile_payments(validated)
    report_task = report()

    # Keep the quarantine task in the critical path while allowing reconciliation
    # to run independently from the star-schema load after validation completes.
    validated >> quarantine_task
    quarantine_task >> transformed
    transformed >> loaded >> report_task
    reconciled >> report_task


nammamart_etl_rudin()

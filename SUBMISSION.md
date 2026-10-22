# NammaMart Submission

## Part E — Data governance

### 1. Ownership

The Operations team owns orders and stores; Finance owns payments and approves reconciliation rules; CRM owns customers; and Catalogue owns products. The Data Engineering/FDE team owns the ETL/ELT warehouse tables, pipeline code, and quality scorecards. Operations, Finance, and Data Engineering should jointly approve a rule change such as the 120-minute delivery limit, with Finance signing off on revenue-impacting changes.

Warehouse ownership is assigned as follows: Operations owns `raw_orders`, `stg_orders`, `raw_stores`, `stg_stores`, and operational marts; Finance owns `raw_payments`, `stg_payments`, payment reconciliation, revenue, and margin marts; CRM owns `raw_customers` and `stg_customers`; Catalogue owns `raw_products` and `stg_products`; and Data Engineering owns the warehouse schemas, `dq_results`, `pipeline_runs`, quarantine process, and all transformation code. Source owners approve source-data rules; Data Engineering applies and monitors them.

### 2. Data dictionary

#### ETL fact and dimensions

| Table | Column | Type | Meaning | Source | Allowed values / derivation | PII |
|---|---|---|---|---|---|---|
| `etl_fact_orders` | `order_id` | VARCHAR | Unique order identifier | `orders.csv` | Source key | No |
| `etl_fact_orders` | `order_ts`, `order_date` | TIMESTAMP, DATE | Order timestamp and reporting date | `orders.csv` | Parsed timestamp / derived date | No |
| `etl_fact_orders` | `customer_id`, `store_id`, `product_id` | VARCHAR | Dimension foreign keys | Orders and master files | Must exist in dimensions | No |
| `etl_fact_orders` | `quantity`, `unit_price`, `discount_pct`, `order_amount` | Numeric | Order quantity, price, discount, and calculated amount | `orders.csv` | Quantity > 0; discount 0–100 | No |
| `etl_fact_orders` | `net_revenue`, `gross_margin` | Numeric | Delivered revenue and revenue minus quantity × cost | Orders/products | Delivered orders only | No |
| `etl_fact_orders` | `order_status`, `payment_mode`, `channel` | VARCHAR | Operational classifications | `orders.csv` | Status: DELIVERED/CANCELLED/RETURNED; payment modes: UPI/Card/Cash on Delivery/Wallet | No |
| `etl_fact_orders` | `delivery_minutes`, `within_15_min` | INTEGER, BOOLEAN | Delivery SLA measures | `orders.csv` | Delivered orders require time; boolean is derived | No |
| `etl_dim_customer` | `customer_id`, `full_name`, `loyalty_tier`, `is_active` | VARCHAR | Customer dimension attributes | `customers.csv` | Loyalty: Bronze/Silver/Gold/Platinum | Name: Yes; other fields: No/related |
| `etl_dim_customer` | `email`, `phone` | VARCHAR | Masked customer contact fields | `customers.csv` | Masked before warehouse load | Yes, masked |
| `etl_dim_product` | `product_id`, `product_name`, `category`, `sub_category`, `brand` | VARCHAR | Product catalogue attributes | `products.csv` | Catalogue values | No |
| `etl_dim_product` | `mrp`, `cost_price`, `unit`, `is_perishable` | Numeric/VARCHAR/BOOLEAN | Product pricing and handling attributes | `products.csv` | `mrp > 0`; cost_price <= mrp | No |
| `etl_dim_store` | `store_id`, `store_name`, `zone`, `area` | VARCHAR | Store and geographic reporting attributes | `stores.csv` | Zone: North/South/East/West | Area may be personal/location-related |

#### ELT raw tables

| Table | Contents and metadata | Purpose / PII |
|---|---|---|
| `raw_orders` | Every order source column as text, plus `_loaded_at`, `_source_file`, `_run_id` | Immutable replay/audit layer; no direct PII |
| `raw_payments` | Every payment source column as text, plus load metadata | Payment replay and reconciliation; no direct PII |
| `raw_customers` | Customer source columns, including name, email, phone, city, area, and pincode, plus load metadata | Restricted source record; contains personal data |
| `raw_products` | Product source columns plus load metadata | Catalogue replay; no direct PII |
| `raw_stores` | Store source columns plus load metadata | Store replay; area may be location-related |

Raw tables are append-only and preserve the original values for audit and replay. `_source_file` identifies the file, `_loaded_at` identifies ingestion time, and `_run_id` identifies the pipeline execution.

#### ELT staging tables

| Table | Key fields and transformations | Quality behaviour |
|---|---|---|
| `stg_orders` | Typed `order_id`, `order_ts`, `order_date`, customer/store/product keys, quantity, price, discount, `order_amount`, `net_revenue`, `gross_margin`, delivery fields, `dq_status`, `failure_reason` | Current order state after `MERGE` on `order_id`; failed rows are excluded from marts |
| `stg_payments` | Typed payment identifiers, order key, amount, mode, status, payment timestamp, `dq_status`, `failure_reason` | Used only when valid and successful for reconciliation |
| `stg_customers` | Typed customer attributes, deduplicated customer key, masked email/phone, `dq_status`, `failure_reason` | Customer PII is masked before analytical use |
| `stg_products` | Typed product attributes, deduplicated product key, price/cost checks, `dq_status`, `failure_reason` | Invalid catalogue rows are excluded from joins |
| `stg_stores` | Typed store attributes, deduplicated store key, `dq_status`, `failure_reason` | Invalid store rows are excluded from joins |

#### ELT marts and control tables

| Table | Grain and measures | Consumers |
|---|---|---|
| `mart_daily_store_kpis` | One row per order date and store; orders, delivered orders, revenue, and margin | Operations and Finance |
| `mart_category_margin` | One row per order date and product category; revenue, cost, and gross margin | Finance and Catalogue |
| `mart_delivery_sla` | One row per order date and store; delivery counts, average delivery time, and within-SLA count | Operations |
| `mart_payment_reconciliation` | One row per order; order amount, successful payment amount, variance, and reconciliation status | Finance |
| `mart_top_customers` | One row per customer; delivered order count, spend, and rank | CRM and approved analysts |
| `dq_results` | One row per quality check and run with `run_id`, `dag_id`, check metadata, counts, status, and `checked_at` | Data Engineering and source owners |
| `pipeline_runs` | One row per DAG run with timing, extracted/loaded/quarantined counts, and status | Data Engineering and audit reviewers |

All marts use passing staging records only. The ETL fact and dimensions above are the parallel star-schema output; the ELT raw, staging, and mart layers are the traceable SQL-based alternative.

### 3. Lineage

For `mart_daily_store_kpis.net_revenue`, the ELT lineage is:

```text
orders.csv / orders_2026-10-01_increment.csv
  -> raw_orders
  -> stg_orders.order_amount, order_status, order_date
  -> mart_daily_store_kpis.net_revenue
```

The mart filters `dq_status = 'PASS'` and `order_status = 'DELIVERED'`, then sums `order_amount` by date and store. ELT is easier to trace because raw rows retain `_source_file`, `_loaded_at`, and `_run_id`. ETL can trace the value through the original CSV and quarantine/fact load, but the raw audit metadata is less direct.

### 4. Personal data and Indian privacy law

The customer export contains `full_name`, `email`, `phone`, `city`, `area`, `pincode`, `signup_date`, `loyalty_tier`, and `is_active`; these can identify or describe a customer and must be access-controlled. The pipelines mask email and phone in reporting dimensions while retaining only the minimum business attributes needed for analysis, rather than deleting them and losing support/audit usefulness.

The relevant framework is India's Digital Personal Data Protection Act, 2023, together with the Digital Personal Data Protection Rules, 2025. NammaMart should establish a lawful processing purpose, provide notice, apply reasonable security safeguards, handle breach obligations and data-principal rights, and erase data when retention is no longer legally or operationally necessary. See the official [DPDP Act](https://www.meity.gov.in/static/uploads/2024/02/Digital-Personal-Data-Protection-Act-2023.pdf) and [DPDP Rules](https://www.meity.gov.in/documents/act-and-policies/digital-personal-data-protection-rules-2025-gDOxUjMtQWa?pageTitle=Digital-Personal-Data-Protection-Rules-2025).

### 5. Access control

Raw tables and raw customer PII should be restricted to approved CRM, security, and data-engineering personnel. Staging tables should be available to data engineering and quality reviewers; marts should be available to Operations, Finance, and approved analysts; quarantine should be available to data engineering and the relevant source-system owner. Unmasked raw phone numbers should not be visible to ordinary analysts or store managers.

### 6. Retention

As a proposed policy, retain raw source tables for 13 months to support year-over-year audit and replay, quarantine rows for 90 days after resolution, and masked customer reporting data for 24 months. At expiry, delete or cryptographically destroy the relevant data after checking legal holds, finance requirements, and open customer requests; retain only aggregated, non-identifying metrics where justified.

### 7. Secrets

Credentials and alert endpoints such as `SLACK_WEBHOOK_URL` should be stored as Airflow Variables or a deployment secret manager and retrieved at runtime with `Variable.get()`. They must not be placed in DAG code, SQL files, notebooks, CSVs, or GitHub because repository history and logs can expose credentials even after a later deletion.

### 8. Audit of a rejected order

In ETL, the rejected row is written to `include/quarantine/rudin/<file>_failed_<run_id>.csv` with `failure_reason`, while the validation task excludes it from `etl_fact_orders`. Finance can compare the quarantine `order_id` against `etl_fact_orders` and confirm it is absent from delivered revenue.

In ELT, the exact source row remains in `raw_orders`; the SQL staging table records `dq_status = 'FAIL'` and `failure_reason`, and every mart filters to passing rows. This gives Finance both the original row and proof that it cannot contribute to `mart_daily_store_kpis` or `mart_category_margin`.

### 9. Scheduling

Both DAGs use the same schedule:

```text
0 9 * * 1-6
```

This means Monday through Saturday at 9:00 AM. The DAG definitions use a timezone-aware `Asia/Kolkata` start date, so the intended business run is **9:00 AM IST**. Airflow displays the equivalent scheduled time as **03:30 UTC** because India Standard Time is UTC+05:30.

Both DAGs also use `catchup=False`. If Docker or the development laptop is offline at 9:00 AM IST, Airflow does not automatically recreate the missed run when the environment comes back online. This is intentional because the morning report should process the current scheduled source snapshot rather than replay every missed schedule. A missed run can still be triggered manually when the source files and environment are available.

Reliability settings are shared by both DAGs: two retries, a two-minute retry delay, a 30-minute execution timeout, and `max_active_runs=1`. The retry settings allow transient failures to recover, the timeout prevents a stuck task from running indefinitely, and the single-active-run limit prevents overlapping writes to the DuckDB warehouses.

### 10. Change history

When an order changes from `DELIVERED` on 3 September to `RETURNED` on 1 October, the ELT raw layer keeps both received records, while the staging `MERGE` makes the latest returned version the current `order_id` state. September current-state revenue therefore excludes the returned order; Finance may also need an as-reported historical view that preserves the original September result.

The recommended design is to keep both views: use the current upserted staging/mart tables for operational truth, and create an append-only historical fact or change-history table keyed by `order_id`, source file, and load timestamp. ETL should similarly retain quarantine/source snapshots or an audit table instead of overwriting the only copy of the prior status.

## Part F — Observability

Both DAGs end with an `all_done` audit task. It writes one row per run to `pipeline_runs` with:

```text
run_id, dag_id, start_time, end_time, rows_extracted,
rows_loaded, rows_quarantined, status
```

The ETL audit counts source extracts, clean fact rows, and quarantine rows. The ELT audit counts raw rows loaded, current staged orders, and rows rejected from staging. A run is marked `failed` when any upstream task is not successful or skipped, including runs where a source file is missing or a schema change causes a task failure.

### Latest ETL/ELT verification

Both DAGs were rerun successfully on 22 October 2026:

| DAG | Run ID | Status | Rows extracted | Rows loaded | Rows quarantined |
|---|---|---|---:|---:|---:|
| `nammamart_etl_rudin` | `manual__2026-10-22T08:36:14.418459+00:00` | success | 5,350 | 2,087 | 149 |
| `nammamart_elt_rudin` | `manual__2026-10-22T08:36:24.460018+00:00` | success | 5,372 | 2,550 | 484 |

The ELT incremental audit records **150 inserts and 10 updates** for `orders_2026-10-01_increment.csv`. Staging contains 2,550 unique orders and no duplicate `order_id` values.

Both derived warehouses were reset and fully reloaded from the source snapshot. The September source data remains available for historical reporting, and both September and October reconciliation tables are now persisted. Source CSVs remain untouched; the clean full load also rebuilt raw ELT tables, quarantine outputs, audit history, and quality logs.

The full-load reconciliation results are:

| Metric | ETL orders | ELT orders | ETL revenue | ELT revenue | Status |
|---|---:|---:|---:|---:|---|
| September delivered revenue | 1,768 | 1,768 | 956,731.20 | 956,731.20 | MATCH |
| October delivered revenue | 128 | 128 | 68,798.20 | 68,798.20 | MATCH |

The full-load comparison now matches. ETL reads the base export plus the October incremental file, applies incremental rows as the latest version for each `order_id`, and publishes 128 valid October delivered orders worth 68,798.20. ELT publishes the same 128 orders and revenue. The one previously inconsistent future-dated order is rejected by both pipelines under the aligned `2026-10-09` cutoff.

### Experiment log

Run each experiment separately in Airflow, restore the original source file after each test, and record the run ID from the DAG run page. The table below records the current evidence. Results marked **pending** still require an isolated mutation run before final submission.

| Experiment | Expected → Observed | Evidence location | Proposed improvement |
|---|---|---|---|
| 1. Happy path | Expected: both DAGs finish successfully → Observed: ETL run `manual__2026-10-22T08:56:01.438957+00:00` and ELT run `manual__2026-10-22T08:56:11.455740+00:00` both finished `success`; audit tasks completed | Airflow run history; `pipeline_runs` | Add alerts if a successful run does not write an audit row |
| 2. Re-run/idempotency | Expected: facts/staging remain deduplicated → Observed: repeated successful runs at 08:36 and 08:56 completed without duplicate staged `order_id` values; ELT raw history appended by design | `etl_fact_orders`, `raw_orders`, `stg_orders`, `pipeline_runs` | Add a raw-load retention or run-deduplication policy if raw growth is not acceptable |
| 3. Incremental load | Expected: 10 updates and 150 inserts → Observed: ELT incremental audit recorded **10 updates and 150 inserts**, with no duplicate staged order keys | `elt_incremental_audit`, `raw_orders`, `stg_orders` | Keep explicit inserted/updated counters in the merge log |
| 4. Missing source file | Expected: retries, final failure, and `all_done` audit → Observed: ETL run `manual__2026-10-22T09:15:39.414302+00:00` ended with `extract_stores = failed`; ELT run `manual__2026-10-22T09:15:52.442352+00:00` ended with `load_raw_stores = failed`; downstream tasks were `upstream_failed`, while `audit_run = success` | Airflow task states and audit logs for the two run IDs; `pipeline_runs` | Keep the failure alert tied to the audit row and source-owner notification |
| 5. New bad data | Expected: invalid rows are quarantined or excluded → Observed: normal source runs quarantined invalid rows with `failure_reason`; ELT `dq_results` recorded critical failures and marts filtered failed staging rows | ETL quarantine CSVs, `stg_orders`, `dq_results` | Add a dedicated mutation run containing one example of each invalid rule |
| 6. Schema change | Expected: required-column change fails visibly → Observed: after renaming `order_amount` to `order_amount_changed`, the ETL extract raised `ValueError: Schema validation failed for orders.csv; missing columns: ['order_amount']`; the equivalent ELT raw-loader check is implemented for the same contract | ETL task-test log; required-column checks in both DAGs; clean verification runs `manual__2026-10-22T10:09:34.700166+00:00` and `manual__2026-10-22T10:09:49.542790+00:00` | Keep required-column contracts version-controlled and alert on schema failures |
| 7. Volume anomaly | Expected: major row-count drop is visible and warned → Observed: **pending isolated low-volume run** | `pipeline_runs`, `dq_results`, task log | Add a persisted baseline-volume check |
| 8. Distribution drift | Expected: price change affects revenue/margin and is detected → Observed: **pending isolated price-drift run** | Mart results, task log, `dq_results` | Add percentile or z-score checks for unit-price drift |
| 9. Freshness failure | Expected: stale orders fail or warn → Observed: future-date timeliness checks are implemented, but a dedicated stale-data run is **pending** | `dq_results`, validation/SQL logs | Make freshness thresholds configurable rather than hard-coded |
| 10. Recovery | Expected: clearing/fixing the failed source allows the failed branch and downstream tasks to rerun → Observed: after restoring `stores.csv`, ETL recovery run `manual__2026-10-22T09:21:11.099122+00:00` and ELT recovery run `manual__2026-10-22T09:21:28.042564+00:00` both completed `success` | Airflow run history, task states, and audit logs | Document the clear-and-retry runbook for operations |

Useful verification queries:

### Final synchronized experiment status

The following corrections supersede the earlier `pending` or generic descriptions in the experiment table above.

| Experiment | Expected | Observed | Evidence location | Proposed improvement |
|---|---|---|---|---|
| 5. New bad data | Five invalid rows should be quarantined or excluded with rule-specific reasons | ETL quarantined the missing-customer, negative-quantity, unknown-store, invalid-status, and future-date rows. ELT completed `record_dq_results` and excluded failed staging rows from marts | ETL run `manual__2026-10-22T11:08:06.112611+00:00`; ELT run `manual__2026-10-22T11:08:22.380751+00:00`; `include/quarantine/rudin/orders_failed_manual__2026-10-22T11_08_06.112611+00_00.csv` | Add a reusable invalid-row fixture covering every critical rule |
| 7. Volume anomaly | A major row-count drop should be visible and warned | The snapshot contained 210 rows, below the 255-row threshold; both pipelines completed and recorded `order_volume_drop` as a warning | ETL run `manual__2026-10-22T10:25:26.809025+00:00`; ELT run `manual__2026-10-22T10:25:38.220706+00:00`; `pipeline_runs` and `dq_results` | Persist a rolling baseline-volume check |
| 8. Distribution drift | A large price change should affect the metric path and be detected | A 10× unit-price/order-amount mutation completed successfully through both DAGs; `price_distribution_drift` was evaluated by ETL and ELT `record_dq_results` | ETL run `manual__2026-10-22T11:13:33.905624+00:00`; ELT run `manual__2026-10-22T11:13:51.253434+00:00`; `dq_results` | Add percentile or z-score monitoring for unit-price drift |
| 9. Freshness failure | A stale order should produce a freshness warning or failure | A `2020-01-01` order timestamp completed through both pipelines; `stale_orders` was evaluated and recorded by the timeliness checks | ETL run `manual__2026-10-22T11:16:39.931370+00:00`; ELT run `manual__2026-10-22T11:16:56.759808+00:00`; `dq_results` | Make the freshness threshold configurable |

Useful verification queries:

Experiment 7 was executed on 22 October 2026 by reducing `orders.csv` to 50 rows while retaining the 160-row increment. ETL run `manual__2026-10-22T10:25:26.809025+00:00` and ELT run `manual__2026-10-22T10:25:38.220706+00:00` completed successfully and recorded the `order_volume_drop` warning because the current snapshot was 210 rows, below the 255-row threshold. The original source file was restored afterward.

Experiment 7 status correction: the main experiment row is superseded by the evidence above and should be treated as **completed**, not pending. The expected result, observed warning, run evidence, and improvement are all recorded here.

Experiment 5 was executed on 22 October 2026 by adding five invalid orders to `orders.csv`: a missing customer, negative quantity, unknown store, invalid `SHIPPED` status, and a 2027 timestamp. ETL run `manual__2026-10-22T11:08:06.112611+00:00` and ELT run `manual__2026-10-22T11:08:22.380751+00:00` both completed successfully. The ETL quarantine file `include/quarantine/rudin/orders_failed_manual__2026-10-22T11_08_06.112611+00_00.csv` contains all five rows with the expected `failure_reason` values; ELT completed `record_dq_results`, and its staging/mart path excludes failed rows. The five test rows were removed and the original source was restored.

Experiment 8 was executed on 22 October 2026 by multiplying one order's unit price and order amount by 10. ETL run `manual__2026-10-22T10:29:09.295941+00:00` and ELT run `manual__2026-10-22T10:29:19.339834+00:00` were submitted with the mutation; both pipelines contain the `price_distribution_drift` warning check, and the original source row was restored afterward. The check compares unit price with twice the product MRP and records the result in `dq_results`.

Experiment 8 status correction: a corrected isolated run was completed on 22 October 2026 using the same 10x price mutation. ETL run `manual__2026-10-22T11:13:33.905624+00:00` and ELT run `manual__2026-10-22T11:13:51.253434+00:00` completed successfully. ETL validation, reporting, and audit succeeded; ELT `record_dq_results`, marts, reconciliation, and audit succeeded. The `price_distribution_drift` warning compares unit price with twice the product MRP, and the original source row was restored afterward.

Experiment 9 was executed on 22 October 2026 by changing one order timestamp to `2020-01-01 00:00:00`. ETL run `manual__2026-10-22T10:30:27.272798+00:00` and ELT run `manual__2026-10-22T10:30:34.502377+00:00` were submitted with the stale timestamp; both pipelines contain the `stale_orders` timeliness warning check, and the original timestamp was restored afterward. The check compares each order with the latest order timestamp minus 30 days and records the result in `dq_results`.

Experiment 9 status correction: a confirmed isolated run was completed on 22 October 2026 using the stale `2020-01-01 00:00:00` timestamp. ETL run `manual__2026-10-22T11:16:39.931370+00:00` and ELT run `manual__2026-10-22T11:16:56.759808+00:00` completed successfully. ETL reporting and audit succeeded; ELT `record_dq_results`, marts, reconciliation, and audit succeeded. The `stale_orders` warning compares each order with the latest order timestamp minus 30 days, and the original source timestamp was restored afterward.

Experiment 10 recovery was completed by restoring `stores.csv` after the missing-file test. ETL run `manual__2026-10-22T09:21:11.099122+00:00` and ELT run `manual__2026-10-22T09:21:28.042564+00:00` both completed successfully.

```sql
SELECT * FROM pipeline_runs ORDER BY end_time DESC;

SELECT * FROM dq_results ORDER BY checked_at DESC;

SELECT order_id, COUNT(*)
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### Five observability pillars

| Pillar | Coverage in this implementation |
|---|---|
| Freshness | `order_ts` and `signup_date` timeliness checks, plus run timestamps in `pipeline_runs` |
| Volume | `rows_extracted`, `rows_loaded`, and `rows_quarantined` in `pipeline_runs`; volume-anomaly experiment |
| Schema | CSV column access and SQL casts fail visibly when required fields disappear; schema-change experiment |
| Distribution | Delivery-time warning and the distribution-drift experiment; price-distribution monitoring is the weakest current check |
| Lineage | ELT raw metadata `_source_file`, `_loaded_at`, `_run_id`, staging `failure_reason`, and ETL quarantine files |

The strongest pillars are lineage and freshness because every ELT raw row carries source metadata and every run records timing and quality results. Distribution is currently weakest because the implementation logs delivery-range anomalies but does not yet persist broad statistical baselines for every numeric field.

## Part G — Recommendation to Sameer, CTO

### Recommendation: adopt ELT with a governed reporting boundary

Sameer,

I recommend ELT as NammaMart's primary analytical architecture, with the SQL staging-to-mart boundary treated as the finance control point. The ELT pipeline keeps every source record in `raw_*` tables exactly as received, then applies typed, documented SQL transformations before data reaches the marts. This gives Analytics and Compliance the auditability they requested while still ensuring that Finance reads only rows with `dq_status = 'PASS'`.

The profiling results support this choice. The six source files contained malformed values, duplicate keys, missing relationships, invalid amounts, impossible delivery times, and future dates. The ELT design preserved those records instead of destroying evidence, while staging recorded `dq_status` and `failure_reason`. The incremental test also behaved as required: the 10 existing order IDs were treated as corrections, the 150 new IDs were inserted, and the staged order count stayed stable on a rerun. The SQL `MERGE` makes the current operational state reproducible while raw history remains available for replay.

### Auditability and data-quality guarantees

ELT is stronger for auditability because each raw row carries `_source_file`, `_loaded_at`, and `_run_id`. A rejected order can therefore be shown exactly as received, linked to its staging failure reason, and proven absent from the marts. The `dq_results` and `pipeline_runs` tables provide a history of checks, counts, task outcomes, and run timing.

ETL provides a simpler immediate guarantee for Finance because invalid rows are rejected before loading. However, it requires the Python validation code to anticipate every future analytical need, and rejected source values must be preserved separately to maintain auditability. In this implementation, ELT achieves the same finance protection by filtering failed staging rows before building `mart_daily_store_kpis`, `mart_category_margin`, and the other marts.

### Ease of change

ELT is easier to change when the business asks a new question or changes a metric: an analyst can add or revise a SQL transformation while retaining the raw source history. For example, Finance can create an as-reported September revenue view alongside the current-state view for orders later returned on 1 October. ETL changes generally require modifying Python validation or transformation code, retesting the full flow, and ensuring that the rejected-row behavior has not changed.

### Scaling to 1,000× the current data

The current DuckDB implementation is appropriate for the assignment and local development, but a 1,000× increase would require moving raw files to partitioned object storage and running DuckDB or a larger analytical warehouse over partitioned Parquet. ELT is the better migration path because raw ingestion, SQL staging, incremental merges, and marts can be partitioned by load date/order date. The current append-only raw pattern also supports replay and backfills without rebuilding the source exports manually.

### Cost

ELT has low initial cost because raw storage and SQL transformations use open-source tools already selected for the project. It avoids maintaining two separate transformation implementations for every new metric. The main future cost is storage and compute for retaining raw history and rerunning SQL models; partition pruning, incremental models, and retention policies should control that cost. ETL may use less warehouse storage initially, but it shifts cost into Python maintenance, exception handling, and repeated development whenever rules change.

### Top three risks and mitigations

1. **Bad SQL could publish incorrect data.** Require critical `dq_results` checks, mart filters, reconciliation thresholds, code review, and row-count/revenue comparison tests against ETL for every release.
2. **Raw data contains sensitive customer information.** Restrict raw access, mask PII in staging, keep secrets outside Git, enforce retention, and audit access to raw customer tables.
3. **Raw-table growth and late corrections could increase cost or confuse historical reporting.** Partition or compact raw storage, use incremental `MERGE` models, retain source/load metadata, and expose separate current-state and as-reported history views.

For NammaMart, ELT provides the best balance of auditability, flexibility, and long-term scale while the quality-gated marts preserve the finance requirement that bad rows never reach executive reporting.

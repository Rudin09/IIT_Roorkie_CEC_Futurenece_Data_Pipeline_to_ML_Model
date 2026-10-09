# NammaMart Submission

## Part E — Data governance

### 1. Ownership

The Operations team owns orders and stores; Finance owns payments and approves reconciliation rules; CRM owns customers; and Catalogue owns products. The Data Engineering/FDE team owns the ETL/ELT warehouse tables, pipeline code, and quality scorecards. Operations, Finance, and Data Engineering should jointly approve a rule change such as the 120-minute delivery limit, with Finance signing off on revenue-impacting changes.

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

The ELT raw tables retain the same source columns as text plus `_loaded_at`, `_source_file`, and `_run_id`. ELT staging casts and masks these fields, while the marts contain only reporting-ready columns.

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

### 9. Change history

When an order changes from `DELIVERED` on 3 September to `RETURNED` on 1 October, the ELT raw layer keeps both received records, while the staging `MERGE` makes the latest returned version the current `order_id` state. September current-state revenue therefore excludes the returned order; Finance may also need an as-reported historical view that preserves the original September result.

The recommended design is to keep both views: use the current upserted staging/mart tables for operational truth, and create an append-only historical fact or change-history table keyed by `order_id`, source file, and load timestamp. ETL should similarly retain quarantine/source snapshots or an audit table instead of overwriting the only copy of the prior status.

## Part F — Observability

Both DAGs end with an `all_done` audit task. It writes one row per run to `pipeline_runs` with:

```text
run_id, dag_id, start_time, end_time, rows_extracted,
rows_loaded, rows_quarantined, status
```

The ETL audit counts source extracts, clean fact rows, and quarantine rows. The ELT audit counts raw rows loaded, current staged orders, and rows rejected from staging. A run is marked `failed` when any upstream task is not successful or skipped, including runs where a source file is missing or a schema change causes a task failure.

### Experiment log

Run each experiment separately in Airflow, restore the original source file after each test, and record the run ID from the DAG run page. The table below defines the expected observation, evidence location, and follow-up. The `Observed` value should be filled with the actual UI/log/table result for the submitted run.

| Experiment | Expected → Observed | Where to see it | What should change |
|---|---|---|---|
| 1. Happy path | Expected: both DAGs finish successfully; extracts run in parallel; row counts appear in logs and `pipeline_runs` → Observed: record actual task states, durations, and counts | Graph view, task logs, `pipeline_runs` | Add alerts if a successful run does not write an audit row |
| 2. Re-run/idempotency | Expected: ETL fact counts remain stable; ELT raw tables append the source snapshot but `stg_orders` and marts remain deduplicated → Observed: compare counts before/after the second run | `etl_fact_orders`, `raw_orders`, `stg_orders`, `pipeline_runs` | Add a raw-load retention or run-deduplication policy if raw growth is not acceptable |
| 3. Incremental load | Expected: 10 existing orders update and 150 new orders insert; no duplicate `order_id` remains in staging → Observed: record merge counts and returned-order count | `raw_orders`, `stg_orders`, ELT task log | Add explicit inserted/updated counters to the merge log if not already visible |
| 4. Missing source file | Expected: extract retries, then fails; downstream tasks become `upstream_failed`; final audit still records `failed` → Observed: record retry count and final states | Graph view, extract log, `pipeline_runs` | Add a failure notification using an Airflow Variable |
| 5. New bad data | Expected: null customer, negative quantity, unknown store, invalid status, and future timestamp are rejected with reasons; ELT raw keeps them → Observed: record five reasons and DQ failures | ETL quarantine CSV, `stg_orders`, `dq_results` | Add or refine rules when a failure reason is ambiguous |
| 6. Schema change | Expected: renaming `order_amount` causes a visible task/SQL failure rather than silent loading → Observed: record failed task and error line | Task log, Graph view, `pipeline_runs` | Add an explicit schema-contract check before raw loading |
| 7. Volume anomaly | Expected: loading only 50 orders is visible in extracted counts; a >90% drop should produce a warning → Observed: record count and warning status | `pipeline_runs`, `dq_results`, task log | Add a baseline volume check if the warning is absent |
| 8. Distribution drift | Expected: multiplying one store's prices changes its revenue/margin and should be detectable through distribution metrics → Observed: record affected store and metric delta | Mart query results, task log, `dq_results` | Add percentile or z-score checks for unit-price drift |
| 9. Freshness failure | Expected: orders 30 days old fail or warn under timeliness checks → Observed: record latest timestamp and DQ result | `dq_results`, validation/SQL logs | Make the freshness threshold configurable rather than hard-coded |
| 10. Recovery | Expected: after fixing the cause and clearing the failed task, only the failed task and downstream tasks rerun → Observed: record task instance attempts and states | Airflow Grid/Graph view, task logs | Document the clear-and-retry runbook for operations |

Useful verification queries:

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

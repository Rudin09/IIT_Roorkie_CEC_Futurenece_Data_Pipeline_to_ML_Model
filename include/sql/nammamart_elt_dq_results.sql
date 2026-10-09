-- Persist one SQL quality-scorecard row for every check on every DAG run.
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
    severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
    status VARCHAR, checked_at TIMESTAMP
);

INSERT INTO dq_results
SELECT '{{ run_id }}', '{{ dag_id }}', check_name, dimension, severity,
       rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS' ELSE 'FAIL' END,
       CURRENT_TIMESTAMP
FROM (
    SELECT 'stg_orders_quality' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE dq_status = 'FAIL') AS rows_failed FROM stg_orders
    UNION ALL
    SELECT 'stg_payments_quality', 'Consistency', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_payments
    UNION ALL
    SELECT 'stg_customers_quality', 'Completeness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_customers
    UNION ALL
    SELECT 'stg_products_quality', 'Accuracy', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_products
    UNION ALL
    SELECT 'stg_stores_quality', 'Completeness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_stores
    UNION ALL
    SELECT 'raw_orders_duplicate_keys', 'Uniqueness', 'Critical', COUNT(*), COUNT(*) - COUNT(DISTINCT order_id) FROM raw_orders
    UNION ALL
    SELECT 'orders_future_dates', 'Timeliness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE TRY_CAST(order_ts AS TIMESTAMP) > TIMESTAMP '2026-10-09 23:59:59') FROM raw_orders
    UNION ALL
    SELECT 'customers_future_dates', 'Timeliness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE TRY_CAST(signup_date AS DATE) > DATE '2026-10-09') FROM raw_customers
    UNION ALL
    SELECT 'customer_pii_format', 'Validity', 'Warning', COUNT(*), COUNT(*) FILTER (WHERE NOT regexp_matches(TRIM(email), '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$') OR NOT regexp_matches(TRIM(phone), '^[0-9]{10}$')) FROM raw_customers
    UNION ALL
    SELECT 'delivery_normal_range', 'Accuracy', 'Warning', COUNT(*), COUNT(*) FILTER (WHERE TRY_CAST(delivery_minutes AS INTEGER) > 35 AND TRY_CAST(delivery_minutes AS INTEGER) < 120) FROM raw_orders
) checks;

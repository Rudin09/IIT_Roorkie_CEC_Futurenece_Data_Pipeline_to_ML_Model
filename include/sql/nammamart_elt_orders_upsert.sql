-- Merge the latest deduplicated order snapshot into persistent staging.
-- Raw is append-only; stg_orders is the current order state used by marts.

-- Recreate the persistent target by name so prior schema versions cannot
-- shift values into the wrong columns during UPDATE SET *.
DROP TABLE IF EXISTS stg_orders;

CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT * FROM stg_orders_snapshot WHERE 1 = 0;

MERGE INTO stg_orders AS target
USING stg_orders_snapshot AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT BY NAME;

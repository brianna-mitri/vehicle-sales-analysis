BEGIN;

/* ------------------- Capture new records only ------------------- */
INSERT INTO etl_watermark (target, last_raw_id)
VALUES ('core_refresh', 0)
ON CONFLICT (target) DO NOTHING;

CREATE TEMP TABLE delta AS
SELECT r.*
FROM raw_orders_csv r
JOIN etl_watermark w ON w.target = 'core_refresh'
WHERE r.raw_id > w.last_raw_id;

/* ------------------- Core tables ------------------- */


/* ------------------- advance watermark ------------------- */
UPDATE etl_watermark
SET last_raw_id = COALESCE((SELECT MAX(raw_id) FROM delta), last_raw_id),
    updated_at = now()
WHERE target = 'core_refresh';


COMMIT;
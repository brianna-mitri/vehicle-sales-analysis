/*--derived rfm data-----------------------------------------------------*/
-- create temp table
CREATE TEMP TABLE tmp_rfm (
    customer_id     bigint,
    recency_days    int,
    frequency       int,
    monetary_amt    numeric(10,2),
    label           varchar(20)
);

-- load the csv
COPY tmp_rfm 
FROM STDIN WITH (FORMAT csv, HEADER true);

/*--load segmentation tables-----------------------------------------------------*/
-- 1st load: rfm_segment_def table
INSERT INTO rfm_segment_def(label)
SELECT DISTINCT label
FROM tmp_rfm
ON CONFLICT DO NOTHING;

--  2nd load: customer_segments table
INSERT INTO customer_segments(
    customer_id,
    segment_id,
    recency_days,
    frequency,
    monetary_amt
)
SELECT
    t.customer_id,
    r.segment_id,
    t.recency_days,
    t.frequency,
    t.monetary_amt
FROM tmp_rfm t
LEFT JOIN rfm_segment_def r ON t.label = r.label
ON CONFLICT (customer_id) DO UPDATE
    SET segment_id      = EXCLUDED.segment_id,
        recency_days    = EXCLUDED.recency_days,
        frequency       = EXCLUDED.frequency,
        monetary_amt    = EXCLUDED.monetary_amt,
        calculated_on   = now();
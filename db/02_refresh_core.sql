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
-- customers
INSERT INTO customers (company_name, contact_last_name, contact_first_name, phone)
SELECT DISTINCT 
    trim(customername), 
    trim(contactlastname), 
    trim(contactfirstname), 
    trim(phone) 
FROM delta
ON CONFLICT DO NOTHING; -- do nothing about rows already loaded

-- addresses
WITH address_src AS (
    SELECT DISTINCT
        c.customer_id,
        trim(d.addressline1)  AS st_addr,
        trim(d.addressline2)  AS sub_addr,
        trim(d.city)          AS city,
        trim(d.state)         AS region,
        trim(d.postalcode)    AS postal_code,
        -- fall back on alias match
        COALESCE(ic.alpha3, ia.alpha3) AS country_code       
    FROM delta d
    LEFT JOIN iso_country_codes ic ON trim(d.country) = ic.name
    LEFT JOIN iso_country_aliases ia ON trim(d.country) = ia.alias
    LEFT JOIN customers c ON d.customername = c.company_name
)
INSERT INTO addresses (
    customer_id, st_addr, sub_addr, city, region, postal_code, country_code)
SELECT customer_id, st_addr, sub_addr, city, region, postal_code, country_code
FROM address_src
ON CONFLICT DO NOTHING;

-- products
WITH product_src AS (
    SELECT 
        trim(productcode)   AS product_code,
        trim(productline)   AS product_line,
        msrp
    FROM delta
)
INSERT INTO products (product_code, product_line, msrp)
SELECT DISTINCT ON (product_code) 
    product_code,
    product_line,
    msrp
FROM product_src
ORDER BY product_code
ON CONFLICT DO NOTHING;

/* ------------------- advance watermark ------------------- */
UPDATE etl_watermark
SET last_raw_id = COALESCE((SELECT MAX(raw_id) FROM delta), last_raw_id),
    updated_at = now()
WHERE target = 'core_refresh';


COMMIT;
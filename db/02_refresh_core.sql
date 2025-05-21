BEGIN;

/* ------------------- Capture new records only ------------------- */
INSERT INTO etl_watermark (target, last_id)
VALUES 
('core_refresh', 0),
('addr_geocode', 0)
ON CONFLICT (target) DO NOTHING;

CREATE TEMP TABLE delta AS
SELECT r.*
FROM raw_orders_csv r
JOIN etl_watermark w ON w.target = 'core_refresh'
WHERE r.raw_id > w.last_id;

-- strip leading/trailing spaces from every text column
DO $$
DECLARE 
    col text;
BEGIN
    FOR col IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'delta'
            AND data_type = 'text'
    LOOP
        EXECUTE format('UPDATE delta SET %I = trim(%I);', col, col);
    END LOOP;
END$$;

/* ------------------- Core tables ------------------- */
-- customers
INSERT INTO customers (company_name, contact_last_name, contact_first_name, phone)
SELECT DISTINCT 
    customername, 
    contactlastname, 
    contactfirstname, 
    phone 
FROM delta
ON CONFLICT DO NOTHING; -- do nothing about rows already loaded

-- addresses
WITH address_src AS (
    SELECT DISTINCT
        c.customer_id,
        d.addressline1  AS st_addr,
        d.addressline2  AS sub_addr,
        d.city,
        d.state         AS region,
        d.postalcode    AS postal_code,
        -- fall back on alias match
        COALESCE(ic.alpha3, ia.alpha3) AS country_code       
    FROM delta d
    LEFT JOIN iso_country_codes ic ON d.country = ic.name
    LEFT JOIN iso_country_aliases ia ON d.country = ia.alias
    LEFT JOIN customers c ON d.customername = c.company_name
)
INSERT INTO addresses (
    customer_id, st_addr, sub_addr, city, region, postal_code, country_code)
SELECT customer_id, st_addr, sub_addr, city, region, postal_code, country_code
FROM address_src
ON CONFLICT DO NOTHING;

-- products
INSERT INTO products (product_code, product_line, msrp)
SELECT DISTINCT productcode, productline, msrp
FROM delta
ON CONFLICT DO NOTHING;

-- orders
WITH order_src AS (
    SELECT DISTINCT
        d.ordernumber       AS order_no,
        c.customer_id,
        a.address_id        AS ship_addr_id,
        d.orderdate         AS order_date,
        d.status,
        d.dealsize          AS deal_size
    FROM delta d
    LEFT JOIN customers c ON d.customername = c.company_name
    LEFT JOIN addresses a ON c.customer_id = a.customer_id
                          AND d.postalcode = a.postal_code
)
INSERT INTO orders (
    order_no, customer_id, ship_addr_id, order_date, status, deal_size)
SELECT order_no, customer_id, ship_addr_id, order_date, status, deal_size
FROM order_src
ORDER BY order_no, order_date
ON CONFLICT DO NOTHING;

-- order_lines
INSERT INTO order_lines (
    order_no, line_no, product_code, quantity, price_each, sales)
SELECT DISTINCT
    ordernumber,
    orderlinenumber,
    productcode,
    quantityordered,
    priceeach,
    sales
FROM delta
ON CONFLICT DO NOTHING;

/* ------------------- advance watermark ------------------- */
UPDATE etl_watermark
SET last_id = COALESCE((SELECT MAX(raw_id) FROM delta), last_id),
    updated_at = now()
WHERE target = 'core_refresh';


COMMIT;
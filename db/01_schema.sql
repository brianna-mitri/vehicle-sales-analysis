/*---------------------------------------------------------
-------------------- CREATE TABLES ------------------------
---------------------------------------------------------*/
/*--landing / raw data-----------------------------------------------------*/
CREATE TABLE IF NOT EXISTS raw_orders_csv (
    raw_id              bigserial PRIMARY KEY,
    ordernumber         int,
    quantityordered     int,
    priceeach           numeric(10,2),
    orderlinenumber     smallint,
    sales               numeric(12,2),
    orderdate           date,
    status              text,
    qtr_id              smallint,
    month_id            smallint,
    year_id             smallint,
    productline         text,
    msrp                numeric(10,2),
    productcode         text,
    customername        text,
    phone               text,
    addressline1        text,
    addressline2        text,
    city                text,
    state               text,
    postalcode          text,
    country             text,
    territory           text,
    contactlastname     text,
    contactfirstname    text,
    dealsize            text,
    created_at          timestamptz DEFAULT now(),
    UNIQUE (ordernumber, orderlinenumber)
);

-- control table
CREATE TABLE IF NOT EXISTS etl_watermark (
    target      text PRIMARY KEY,
    last_id     bigint,             
    updated_at  timestamptz DEFAULT now()
);------------------------------------------------------------------------

/*--iso country codes-----------------------------------------------------*/
CREATE TABLE IF NOT EXISTS iso_country_codes (
    alpha3          char(3)     PRIMARY KEY,
    alpha2          char(2)     UNIQUE,
    name            varchar(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS iso_country_aliases (
    alias           text PRIMARY KEY,
    alpha3          char(3) REFERENCES iso_country_codes(alpha3)
); -----------------------------------------------------------------------


/*--core tables-----------------------------------------------------*/
CREATE TABLE IF NOT EXISTS customers (
    customer_id         bigserial PRIMARY KEY,
    company_name        varchar(80),
    contact_last_name   text,
    contact_first_name  text,
    phone               varchar(25),
    created_at          timestamptz DEFAULT now(),
    updated_at          timestamptz DEFAULT now(),

    -- prevent duplicate companies
    CONSTRAINT uc_company_name UNIQUE (company_name)
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id          bigserial PRIMARY KEY,
    customer_id         bigint REFERENCES customers(customer_id),
    st_addr             text,
    sub_addr            text,
    city                text,
    region              text,
    postal_code         text,
    country_code        char(3) REFERENCES iso_country_codes(alpha3),
    score               numeric(5,2),
    --match_addr           text,
    created_at          timestamptz DEFAULT now(),  --when row first inserted       
    updated_at          timestamptz DEFAULT now(),   --auto-updated by trigger

    -- unique constraint to prevent duplicate addresses
    CONSTRAINT uc_cust_address UNIQUE (customer_id, st_addr, postal_code)
);

CREATE TABLE IF NOT EXISTS products (
    product_code    varchar(20) PRIMARY KEY,
    product_line    varchar(30),
    msrp            numeric(10,2)
);

CREATE TABLE IF NOT EXISTS orders (
    order_no            int PRIMARY KEY,
    customer_id         bigint REFERENCES customers(customer_id),
    ship_addr_id        bigint REFERENCES addresses(address_id),
    order_date          date,
    status              varchar(10)
        CONSTRAINT chk_status
        CHECK (status IN (
            'Shipped', 'Resolved', 'Cancelled', 
            'On Hold', 'Disputed', 'In Process'
        )),
    deal_size           varchar(6)
        CONSTRAINT chk_deal_size
        CHECK (deal_size IN (
            'Medium', 'Small', 'Large'
        ))
);

CREATE TABLE IF NOT EXISTS order_lines (
    order_no            int REFERENCES orders(order_no),            -- parent/child relationship with orders (parent)
    line_no             smallint,
    product_code        varchar(20) REFERENCES products(product_code),
    quantity            int,
    price_each          numeric(10,2),
    sales               numeric(12,2),
    PRIMARY KEY (order_no, line_no)
); -----------------------------------------------------------------------


/*--audit tables-----------------------------------------------------*/
CREATE TABLE IF NOT EXISTS customers_audit (
    audit_id            bigserial PRIMARY KEY,
    customer_id         bigint REFERENCES customers(customer_id),
    changed_at          timestamptz DEFAULT now(),
    changed_by          varchar(30) NOT NULL,
    operation           char(1),                    --'I' insert; or 'U' update; or 'D' delete

    -- columns preserved
    company_name        varchar(80),
    contact_last_name   text,
    contact_first_name  text,
    phone               varchar(25)
);

CREATE TABLE IF NOT EXISTS addresses_audit (
    audit_id            bigserial PRIMARY KEY,
    address_id          bigint REFERENCES addresses(address_id),
    changed_at          timestamptz DEFAULT now(),
    changed_by          varchar(30) NOT NULL,        
    operation           char(1),                    --'I' insert; or 'U' update; or 'D' delete

    -- columns preserved
    st_addr             text,
    sub_addr            text,
    city                text,
    region              text,
    postal_code         text,
    country_code        char(3),
    score               numeric(5,2)
    --full_addr           text,
);------------------------------------------------------------------------


/*---------------------------------------------------------
---------------------- TRIGGERS ---------------------------
---------------------------------------------------------*/
/*--auto-refresh updated_at-----------------------------------------------------*/
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- for customers
CREATE TRIGGER trg_cust_touch_updated
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- for addresses
CREATE TRIGGER trg_addr_touch_updated
BEFORE UPDATE ON addresses
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();------------------------------------------------------------------------


/*--customers: capture old values for updating/deleting and new for insert-----------------------------------------------------*/
CREATE OR REPLACE FUNCTION audit_customers()
RETURNS trigger AS $$
BEGIN
    -- ignore updates with no changes
    IF TG_OP = 'UPDATE'
        AND NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;

    INSERT INTO customers_audit (
        customer_id,
        operation,
        changed_by,
        company_name,
        contact_last_name,
        contact_first_name,
        phone
    )
    
    VALUES (
        COALESCE(NEW.customer_id, OLD.customer_id),
        -- get one character
        CASE TG_OP
            WHEN  'INSERT' THEN 'I'
            WHEN  'UPDATE' THEN 'U'
            ELSE                'D'
        END,
        current_user,

        -- new value for insert and old value for update/delete
        CASE WHEN TG_OP = 'DELETE' THEN OLD.company_name            ELSE NEW.company_name END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.contact_last_name       ELSE NEW.contact_last_name END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.contact_first_name      ELSE NEW.contact_first_name END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.phone                   ELSE NEW.phone END
    );
    -- return correct row depending on IUD (I/U --> return new; D --> return old)
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- fire trigger after an audit per record
CREATE TRIGGER trg_customers_audit
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION audit_customers();------------------------------------------------------------------------


/*--addresses: capture old values for updating/deleting and new for insert-----------------------------------------------------*/
CREATE OR REPLACE FUNCTION audit_addresses()
RETURNS trigger AS $$
BEGIN
    -- ignore updates with no changes
    IF TG_OP = 'UPDATE'
        AND NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    
    INSERT INTO addresses_audit (
        address_id,
        operation, 
        changed_by, 
        st_addr, 
        sub_addr, 
        city, 
        region, 
        postal_code, 
        country_code,
        score
    )

    VALUES (
        COALESCE(NEW.address_id, OLD.address_id),
        -- get one character
        CASE TG_OP
            WHEN  'INSERT' THEN 'I'
            WHEN  'UPDATE' THEN 'U'
            ELSE                'D'
        END,
        current_user,

        -- new value for insert and old for update/delete
        CASE WHEN TG_OP = 'DELETE' THEN OLD.st_addr         ELSE NEW.st_addr END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.sub_addr        ELSE NEW.sub_addr END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.city            ELSE NEW.city END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.region          ELSE NEW.region END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.postal_code     ELSE NEW.postal_code END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.country_code    ELSE NEW.country_code END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.score           ELSE NEW.score END
    );
    -- return correct row depending on IUD (I/U --> return new; D --> return old)
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql; 

-- fire trigger after an audit per record
CREATE TRIGGER trg_addresses_audit
AFTER INSERT OR UPDATE OR DELETE ON addresses
FOR EACH ROW EXECUTE FUNCTION audit_addresses();------------------------------------------------------------------------

/*---------------------------------------------------------
------------------ CREATE EXTENSIONS ----------------------
---------------------------------------------------------*/
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
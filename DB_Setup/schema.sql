/*--------------------------------------
CREATE TABLES
--------------------------------------*/
-- landing / raw data 
CREATE TABLE raw_orders_csv (
    ordernumber         int,
    quantity_ordered    int,
    price_each          numeric(10,2),
    order_line_number   smallint,
    sales               numeric(12,2),
    order_date          date,
    status              text,
    qtr_id              smallint,
    month_id            smallint,
    year_id             smallint,
    productline         text,
    msrp                numeric(10,2),
    product_code        text,
    customer_name       text,
    phone               text,
    addressline1        text,
    addressline2        text,
    city                text,
    state               text,
    postal_code         text,
    country             text,
    territory           text,
    contact_last_name   text,
    contact_first_name  text,
    deal_size           text,
    created_at          timestamptz DEFAULT now()
);


-- core tables
CREATE TABLE customers (
    customer_id         bigserial PRIMARY KEY,
    contact_last_name   text,
    contact_first_name  text,
    phone               text,
    created_at          timestamptz DEFAULT now()
);

CREATE TABLE addresses (
    address_id          bigserial PRIMARY KEY,
    customer_id         bigint REFERENCES customers(customer_id),
    st_addr             text,
    sub_addr            text,
    city                text,
    region              text,
    postal_code         text,
    country_code        char(3),
    --match_addr           text,
    created_at          timestamptz DEFAULT now(),  --when row first inserted       
    updated_at          timestamptz DEFAULT now()   --auto-updated by trigger

    -- unique constraint to prevent duplicate addresses
    CONSTRAINT uc_cust_address UNIQUE (customer_id, st_addr, postal_code)
);

-- audit table
CREATE TABLE addresses_audit (
    audit_id            bigserial PRIMARY KEY,
    address_id          bigint REFERENCES addresses(address_id),
    changed_at          timestamptz DEFAULT now(),
    changed_by          varchar(100) NOT NULL,        
    operation           char(1),                    --'I' insert; or 'U' update; or 'D' delete

    -- columns preserved
    st_addr             text,
    sub_addr            text,
    city                text,
    region              text,
    postal_code         text,
    country_code        char(3)
    --full_addr           text,
);

/*--------------------------------------
TRIGGERS
--------------------------------------*/
-- auto-refresh updated_at
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_touch_updated
BEFORE UPDATE ON addresses
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- audit addresses table trigger
CREATE OR REPLACE FUNCTION audit_addresses()
RETURNS trigger AS $$
BEGIN
    INSERT INTO addresses_audit (
        address_id,
        operation, 
        changed_by, 
        st_addr, 
        sub_addr, 
        city, 
        region, 
        postal_code, 
        country_code
    )

    VALUES (
        COALESCE(OLD.address_id, NEW.address_id),
        TG_OP,
        current_user,

        -- new value for insert and old for update/delete
        CASE WHEN TG_OP = 'INSERT' THEN NEW.st_addr         ELSE OLD.st_addr END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.sub_addr        ELSE OLD.sub_addr END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.city            ELSE OLD.city END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.region          ELSE OLD.region END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.postal_code     ELSE OLD.postal_code END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.country_code    ELSE OLD.country_code END  
        -- COALESCE(OLD.st_addr, NEW.st_addr),
        -- COALESCE(OLD.sub_addr, NEW.sub_addr),
        -- COALESCE(OLD.city, NEW.city),
        -- COALESCE(OLD.region, NEW.region),
        -- COALESCE(OLD.postal_code, NEW.postal_code),
        -- COALESCE(OLD.country_code, NEW.country_code)
    );
    -- return correct row depending on IUD (I/U --> return new; D --> return old)
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- fire trigger after an audit per record
CREATE TRIGGER trg_addresses_audit
AFTER INSERT OR UPDATE OR DELETE ON addresses
FOR EACH ROW EXECUTE FUNCTION audit_addresses();
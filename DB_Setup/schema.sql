/*---------------------------------------------------------
-------------------- CREATE TABLES ------------------------
---------------------------------------------------------*/
/*--landing / raw data-----------------------------------------------------*/
CREATE TABLE raw_orders_csv (
    raw_id              bigserial PRIMARY KEY,
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
    created_at          timestamptz DEFAULT now(),
    UNIQUE (ordernumber, order_line_number)
);------------------------------------------------------------------------


/*--core tables-----------------------------------------------------*/
CREATE TABLE customers (
    customer_id         bigserial PRIMARY KEY,
    company_name        varchar(80),
    contact_last_name   text,
    contact_first_name  text,
    phone               varchar(25),
    created_at          timestamptz DEFAULT now(),
    updated_at          timestamptz DEFAULT now()
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
    updated_at          timestamptz DEFAULT now(),   --auto-updated by trigger

    -- unique constraint to prevent duplicate addresses
    CONSTRAINT uc_cust_address UNIQUE (customer_id, st_addr, postal_code)
);

CREATE TABLE products (
    product_code    varchar(20) PRIMARY KEY,
    product_line    varchar(30),
    msrp            numeric(10,2)
);

CREATE TABLE orders (
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

CREATE TABLE order_lines (
    order_no            int REFERENCES orders(order_no),            -- parent/child relationship with orders (parent)
    line_no             smallint,
    product_code        varchar(20) REFERENCES products(product_code),
    quantity            int,
    price_each          numeric(10,2),
    sales               numeric(12,2),
    PRIMARY KEY (order_no, line_no)
);-----------------------------------------------------------------------

/*--audit tables-----------------------------------------------------*/
CREATE TABLE customers_audit (
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

CREATE TABLE addresses_audit (
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
    country_code        char(3)
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
        COALESCE(OLD.customer_id, NEW.customer_id),
        TG_OP,
        current_user,

        -- new value for insert and old value for update/delete
        CASE WHEN TG_OP = 'INSERT' THEN NEW.company_name            ELSE OLD.company_name END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.contact_last_name       ELSE OLD.contact_last_name END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.contact_first_name      ELSE OLD.contact_first_name END,
        CASE WHEN TG_OP = 'INSERT' THEN NEW.phone                   ELSE OLD.phone END
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
    );
    -- return correct row depending on IUD (I/U --> return new; D --> return old)
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql; 

-- fire trigger after an audit per record
CREATE TRIGGER trg_addresses_audit
AFTER INSERT OR UPDATE OR DELETE ON addresses
FOR EACH ROW EXECUTE FUNCTION audit_addresses();------------------------------------------------------------------------
/*
================================================================================
STAGING: CSV REFERENCE DATA
================================================================================
Purpose: Parse and normalize CSV reference files
Author:  Data Platform Team
Version: 2.0

Handles:
- Customers from Customer.csv
- Orders from Orders.csv
- Products from Products.csv
- Payments from Payments.csv
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA staging;

-- ============================================================================
-- VIEW: Customers
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_customers AS
SELECT
    -- Identifiers
    raw_record_id,
    source_file_name,
    batch_id,
    file_row_number,
    
    -- Raw Fields
    customer_id_raw,
    first_name_raw,
    last_name_raw,
    email_raw,
    loyalty_tier_raw,
    signup_source_raw,
    is_active_raw,
    
    -- Normalized Fields
    UPPER(TRIM(NULLIF(customer_id_raw, ''))) AS customer_id,
    INITCAP(TRIM(NULLIF(first_name_raw, ''))) AS first_name,
    INITCAP(TRIM(NULLIF(last_name_raw, ''))) AS last_name,
    LOWER(TRIM(NULLIF(email_raw, ''))) AS email,
    
    -- Email Validation
    CASE 
        WHEN NULLIF(TRIM(email_raw), '') IS NULL THEN 'MISSING'
        WHEN NOT REGEXP_LIKE(LOWER(TRIM(email_raw)), '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') THEN 'INVALID'
        ELSE 'VALID'
    END AS email_validation_status,
    
    -- Loyalty Tier Normalization
    CASE 
        WHEN UPPER(TRIM(COALESCE(loyalty_tier_raw, ''))) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') 
        THEN UPPER(TRIM(loyalty_tier_raw))
        ELSE 'UNKNOWN'
    END AS loyalty_tier,
    
    TRIM(NULLIF(signup_source_raw, '')) AS signup_source,
    
    -- Boolean conversion
    CASE 
        WHEN UPPER(TRIM(COALESCE(is_active_raw, ''))) IN ('TRUE', 'YES', '1', 'Y', 'T') THEN TRUE
        WHEN UPPER(TRIM(COALESCE(is_active_raw, ''))) IN ('FALSE', 'NO', '0', 'N', 'F') THEN FALSE
        ELSE NULL
    END AS is_active,
    
    -- DQ Flags
    CASE WHEN NULLIF(TRIM(customer_id_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_customer_id,
    CASE WHEN NULLIF(TRIM(email_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_email,
    CASE WHEN NULLIF(TRIM(first_name_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_first_name,
    CASE WHEN NULLIF(TRIM(last_name_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_last_name,
    
    -- Record Hash
    record_hash

FROM raw.raw_csv_customers;

-- ============================================================================
-- VIEW: Orders
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_orders AS
SELECT
    -- Identifiers
    raw_record_id,
    source_file_name,
    batch_id,
    file_row_number,
    
    -- Raw Fields
    order_id_raw,
    customer_id_raw,
    order_date_raw,
    status_raw,
    total_amount_raw,
    currency_raw,
    
    -- Normalized Fields
    UPPER(TRIM(NULLIF(order_id_raw, ''))) AS order_id,
    UPPER(TRIM(NULLIF(customer_id_raw, ''))) AS customer_id,
    TRY_TO_DATE(order_date_raw) AS order_date,
    UPPER(TRIM(NULLIF(status_raw, ''))) AS status,
    TRY_TO_DECIMAL(total_amount_raw, 18, 4) AS total_amount,
    ABS(TRY_TO_DECIMAL(total_amount_raw, 18, 4)) AS total_amount_abs,
    COALESCE(UPPER(TRIM(NULLIF(currency_raw, ''))), 'USD') AS currency,
    
    -- DQ Flags
    CASE WHEN NULLIF(TRIM(order_id_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_order_id,
    CASE WHEN NULLIF(TRIM(customer_id_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_customer_id,
    CASE WHEN NULLIF(TRIM(order_date_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_order_date,
    CASE WHEN TRY_TO_DATE(order_date_raw) IS NULL AND NULLIF(TRIM(order_date_raw), '') IS NOT NULL THEN TRUE ELSE FALSE END AS is_invalid_order_date,
    CASE WHEN TRY_TO_DECIMAL(total_amount_raw, 18, 4) < 0 THEN TRUE ELSE FALSE END AS is_negative_amount,
    
    -- Record Hash
    record_hash

FROM raw.raw_csv_orders;

-- ============================================================================
-- VIEW: Products
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_products AS
SELECT
    -- Identifiers
    raw_record_id,
    source_file_name,
    batch_id,
    file_row_number,
    
    -- Raw Fields
    product_id_raw,
    sku_raw,
    product_name_raw,
    description_raw,
    category_raw,
    price_raw,
    currency_raw,
    
    -- Normalized Fields
    UPPER(TRIM(NULLIF(product_id_raw, ''))) AS product_id,
    UPPER(TRIM(NULLIF(sku_raw, ''))) AS sku,
    TRIM(NULLIF(product_name_raw, '')) AS product_name,
    TRIM(NULLIF(description_raw, '')) AS description,
    INITCAP(TRIM(NULLIF(category_raw, ''))) AS category,
    TRY_TO_DECIMAL(price_raw, 18, 4) AS price,
    ABS(TRY_TO_DECIMAL(price_raw, 18, 4)) AS price_abs,
    COALESCE(UPPER(TRIM(NULLIF(currency_raw, ''))), 'USD') AS currency,
    
    -- DQ Flags
    CASE WHEN NULLIF(TRIM(sku_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_sku,
    CASE WHEN NULLIF(TRIM(product_name_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_product_name,
    CASE WHEN TRY_TO_DECIMAL(price_raw, 18, 4) < 0 THEN TRUE ELSE FALSE END AS is_negative_price,
    
    -- Record Hash
    record_hash

FROM raw.raw_csv_products;

-- ============================================================================
-- VIEW: Payments
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_payments AS
SELECT
    -- Identifiers
    raw_record_id,
    source_file_name,
    batch_id,
    file_row_number,
    
    -- Raw Fields
    payment_id_raw,
    order_id_raw,
    transaction_id_raw,
    payment_method_raw,
    amount_raw,
    currency_raw,
    payment_date_raw,
    status_raw,
    
    -- Normalized Fields
    UPPER(TRIM(NULLIF(payment_id_raw, ''))) AS payment_id,
    UPPER(TRIM(NULLIF(order_id_raw, ''))) AS order_id,
    UPPER(TRIM(NULLIF(transaction_id_raw, ''))) AS transaction_id,
    
    -- Payment Method Normalization
    CASE 
        WHEN UPPER(REPLACE(TRIM(COALESCE(payment_method_raw, '')), ' ', '')) IN ('CREDITCARD', 'CREDIT_CARD', 'CC') THEN 'CREDIT_CARD'
        WHEN UPPER(REPLACE(TRIM(COALESCE(payment_method_raw, '')), ' ', '')) IN ('DEBITCARD', 'DEBIT_CARD', 'DC') THEN 'DEBIT_CARD'
        WHEN UPPER(TRIM(COALESCE(payment_method_raw, ''))) = 'PAYPAL' THEN 'PAYPAL'
        WHEN UPPER(REPLACE(TRIM(COALESCE(payment_method_raw, '')), ' ', '')) IN ('BANKTRANSFER', 'BANK_TRANSFER', 'WIRE') THEN 'BANK_TRANSFER'
        WHEN NULLIF(TRIM(payment_method_raw), '') IS NULL THEN 'MISSING'
        ELSE 'OTHER'
    END AS payment_method,
    
    TRY_TO_DECIMAL(amount_raw, 18, 4) AS amount,
    ABS(TRY_TO_DECIMAL(amount_raw, 18, 4)) AS amount_abs,
    COALESCE(UPPER(TRIM(NULLIF(currency_raw, ''))), 'USD') AS currency,
    TRY_TO_DATE(payment_date_raw) AS payment_date,
    UPPER(TRIM(NULLIF(status_raw, ''))) AS status,
    
    -- DQ Flags
    CASE WHEN NULLIF(TRIM(payment_id_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment_id,
    CASE WHEN NULLIF(TRIM(order_id_raw), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_order_id,
    CASE WHEN TRY_TO_DECIMAL(amount_raw, 18, 4) < 0 THEN TRUE ELSE FALSE END AS is_negative_amount

FROM raw.raw_csv_payments;

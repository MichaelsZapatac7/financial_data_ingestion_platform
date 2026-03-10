/*
================================================================================
STAGING: CLIENT C TRANSACTIONS (JSON)
================================================================================
Purpose: Parse and flatten JSON transactions from ClientC
Author:  Data Platform Team
Version: 2.0

Handles:
- JSON parsing using colon notation and LATERAL FLATTEN
- Multiple field name variants (snake_case, camelCase)
- Nested customer, item, and payment extraction
- Comprehensive data quality flags
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA staging;

-- ============================================================================
-- VIEW: Parse JSON Transactions from ClientC
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_client_c_transactions AS
WITH parsed_json AS (
    SELECT
        r.raw_record_id,
        r.source_file_name,
        r.source_client,
        r.source_format,
        r.ingestion_timestamp,
        r.batch_id,
        r.record_hash AS file_hash,
        r.raw_json_content,
        txn.value AS transaction_json,
        txn.index AS transaction_index
    FROM raw.raw_json_transactions r,
    LATERAL FLATTEN(
        INPUT => r.raw_json_content:transactions,
        OUTER => TRUE
    ) txn
    WHERE r.source_client = 'ClientC'
      AND txn.value IS NOT NULL
)

SELECT
    -- ================================================================
    -- IDENTIFIERS
    -- ================================================================
    p.raw_record_id,
    p.source_file_name,
    p.source_client,
    p.source_format,
    p.ingestion_timestamp,
    p.batch_id,
    p.file_hash,
    p.transaction_index,
    
    -- ================================================================
    -- RAW FIELDS (as extracted from JSON)
    -- ================================================================
    -- Transaction Level (handle multiple field names)
    p.transaction_json:transaction_id::VARCHAR AS transaction_id_raw,
    p.transaction_json:txn_id::VARCHAR AS transaction_id_alt_raw,
    
    -- Order Level
    p.transaction_json:order:order_id::VARCHAR AS order_id_raw,
    p.transaction_json:order:order_date::VARCHAR AS order_date_raw,
    p.transaction_json:order:status::VARCHAR AS order_status_raw,
    
    -- Customer Level (handle multiple field names)
    p.transaction_json:order:customer:customer_id::VARCHAR AS customer_id_raw,
    p.transaction_json:order:customer:id::VARCHAR AS customer_id_alt_raw,
    p.transaction_json:order:customer:first_name::VARCHAR AS first_name_raw,
    p.transaction_json:order:customer:last_name::VARCHAR AS last_name_raw,
    p.transaction_json:order:customer:name::VARCHAR AS full_name_raw,
    p.transaction_json:order:customer:email::VARCHAR AS email_raw,
    p.transaction_json:order:customer:loyalty_tier::VARCHAR AS loyalty_tier_raw,
    p.transaction_json:order:customer:tier::VARCHAR AS tier_alt_raw,
    
    -- Payment Level (handle multiple field names)
    p.transaction_json:payment:method::VARCHAR AS payment_method_raw,
    p.transaction_json:payment:payment_method::VARCHAR AS payment_method_alt_raw,
    p.transaction_json:payment:amount::VARCHAR AS payment_amount_raw,
    p.transaction_json:payment:total::VARCHAR AS payment_total_alt_raw,
    p.transaction_json:payment:currency::VARCHAR AS payment_currency_raw,
    
    -- Items Array
    p.transaction_json:items AS items_json,
    
    -- Full Objects for metadata
    p.transaction_json:order:customer AS customer_json,
    p.transaction_json:payment AS payment_json,
    
    -- ================================================================
    -- NORMALIZED FIELDS
    -- ================================================================
    -- Transaction ID (COALESCE multiple field names)
    UPPER(TRIM(NULLIF(COALESCE(
        p.transaction_json:transaction_id::VARCHAR,
        p.transaction_json:txn_id::VARCHAR
    ), ''))) AS transaction_id,
    
    -- Order ID & Date
    UPPER(TRIM(NULLIF(p.transaction_json:order:order_id::VARCHAR, ''))) AS order_id,
    TRY_TO_DATE(p.transaction_json:order:order_date::VARCHAR) AS order_date,
    
    -- Customer ID
    UPPER(TRIM(NULLIF(COALESCE(
        p.transaction_json:order:customer:customer_id::VARCHAR,
        p.transaction_json:order:customer:id::VARCHAR
    ), ''))) AS customer_id,
    
    -- Customer Names (with full_name fallback parsing)
    CASE 
        WHEN NULLIF(p.transaction_json:order:customer:first_name::VARCHAR, '') IS NOT NULL 
        THEN INITCAP(TRIM(p.transaction_json:order:customer:first_name::VARCHAR))
        WHEN NULLIF(p.transaction_json:order:customer:name::VARCHAR, '') IS NOT NULL
        THEN INITCAP(TRIM(SPLIT_PART(p.transaction_json:order:customer:name::VARCHAR, ' ', 1)))
        ELSE NULL
    END AS first_name,
    
    CASE 
        WHEN NULLIF(p.transaction_json:order:customer:last_name::VARCHAR, '') IS NOT NULL 
        THEN INITCAP(TRIM(p.transaction_json:order:customer:last_name::VARCHAR))
        WHEN NULLIF(p.transaction_json:order:customer:name::VARCHAR, '') IS NOT NULL
        THEN INITCAP(TRIM(
            CASE 
                WHEN ARRAY_SIZE(SPLIT(p.transaction_json:order:customer:name::VARCHAR, ' ')) > 1
                THEN SPLIT_PART(p.transaction_json:order:customer:name::VARCHAR, ' ', 2)
                ELSE ''
            END
        ))
        ELSE NULL
    END AS last_name,
    
    -- Email
    LOWER(TRIM(NULLIF(p.transaction_json:order:customer:email::VARCHAR, ''))) AS email,
    
    -- Email Validation
    CASE 
        WHEN NULLIF(TRIM(p.transaction_json:order:customer:email::VARCHAR), '') IS NULL THEN 'MISSING'
        WHEN NOT REGEXP_LIKE(
            LOWER(TRIM(p.transaction_json:order:customer:email::VARCHAR)), 
            '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        ) THEN 'INVALID'
        ELSE 'VALID'
    END AS email_validation_status,
    
    -- Loyalty Tier
    CASE 
        WHEN UPPER(TRIM(COALESCE(
            p.transaction_json:order:customer:loyalty_tier::VARCHAR,
            p.transaction_json:order:customer:tier::VARCHAR
        ))) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') 
        THEN UPPER(TRIM(COALESCE(
            p.transaction_json:order:customer:loyalty_tier::VARCHAR,
            p.transaction_json:order:customer:tier::VARCHAR
        )))
        ELSE 'UNKNOWN'
    END AS loyalty_tier,
    
    -- Payment Method Normalization
    CASE 
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            p.transaction_json:payment:method::VARCHAR,
            p.transaction_json:payment:payment_method::VARCHAR
        )), ' ', '')) IN ('CREDITCARD', 'CREDIT_CARD', 'CC') THEN 'CREDIT_CARD'
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            p.transaction_json:payment:method::VARCHAR,
            p.transaction_json:payment:payment_method::VARCHAR
        )), ' ', '')) IN ('DEBITCARD', 'DEBIT_CARD', 'DC') THEN 'DEBIT_CARD'
        WHEN UPPER(TRIM(COALESCE(
            p.transaction_json:payment:method::VARCHAR,
            p.transaction_json:payment:payment_method::VARCHAR
        ))) = 'PAYPAL' THEN 'PAYPAL'
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            p.transaction_json:payment:method::VARCHAR,
            p.transaction_json:payment:payment_method::VARCHAR
        )), ' ', '')) IN ('BANKTRANSFER', 'BANK_TRANSFER', 'WIRE') THEN 'BANK_TRANSFER'
        WHEN NULLIF(TRIM(COALESCE(
            p.transaction_json:payment:method::VARCHAR,
            p.transaction_json:payment:payment_method::VARCHAR
        )), '') IS NULL THEN 'MISSING'
        ELSE 'OTHER'
    END AS payment_method,
    
    -- Payment Amount
    TRY_TO_DECIMAL(COALESCE(
        p.transaction_json:payment:amount::VARCHAR,
        p.transaction_json:payment:total::VARCHAR
    ), 18, 4) AS payment_amount,
    
    ABS(TRY_TO_DECIMAL(COALESCE(
        p.transaction_json:payment:amount::VARCHAR,
        p.transaction_json:payment:total::VARCHAR
    ), 18, 4)) AS payment_amount_abs,
    
    -- Payment Currency
    COALESCE(UPPER(TRIM(NULLIF(p.transaction_json:payment:currency::VARCHAR, ''))), 'USD') AS payment_currency,
    
    -- Items Count
    COALESCE(ARRAY_SIZE(p.transaction_json:items), 0) AS items_count,
    
    -- ================================================================
    -- METADATA EXTRACTION
    -- ================================================================
    -- Customer extra fields (anything not explicitly mapped)
    OBJECT_CONSTRUCT_KEEP_NULL(
        'source_file', p.source_file_name,
        'has_loyalty_tier', p.transaction_json:order:customer:loyalty_tier IS NOT NULL,
        'order_status', p.transaction_json:order:status::VARCHAR
    ) AS transaction_metadata,
    
    -- ================================================================
    -- DATA QUALITY FLAGS
    -- ================================================================
    CASE WHEN NULLIF(TRIM(COALESCE(
        p.transaction_json:transaction_id::VARCHAR,
        p.transaction_json:txn_id::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_transaction_id,
    
    CASE WHEN NULLIF(TRIM(p.transaction_json:order:order_id::VARCHAR), '') IS NULL 
         THEN TRUE ELSE FALSE END AS is_missing_order_id,
    
    CASE WHEN NULLIF(TRIM(p.transaction_json:order:order_date::VARCHAR), '') IS NULL 
         THEN TRUE ELSE FALSE END AS is_missing_order_date,
    
    CASE WHEN TRY_TO_DATE(p.transaction_json:order:order_date::VARCHAR) IS NULL
          AND NULLIF(TRIM(p.transaction_json:order:order_date::VARCHAR), '') IS NOT NULL
         THEN TRUE ELSE FALSE END AS is_invalid_order_date,
    
    CASE WHEN NULLIF(TRIM(COALESCE(
        p.transaction_json:order:customer:customer_id::VARCHAR,
        p.transaction_json:order:customer:id::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_customer_id,
    
    CASE WHEN NULLIF(TRIM(p.transaction_json:order:customer:email::VARCHAR), '') IS NULL 
         THEN TRUE ELSE FALSE END AS is_missing_email,
    
    CASE WHEN NULLIF(TRIM(COALESCE(
        p.transaction_json:payment:method::VARCHAR,
        p.transaction_json:payment:payment_method::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment_method,
    
    CASE WHEN NULLIF(TRIM(COALESCE(
        p.transaction_json:payment:amount::VARCHAR,
        p.transaction_json:payment:total::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment_amount,
    
    CASE WHEN TRY_TO_DECIMAL(COALESCE(
        p.transaction_json:payment:amount::VARCHAR,
        p.transaction_json:payment:total::VARCHAR
    ), 18, 4) < 0 THEN TRUE ELSE FALSE END AS is_negative_payment_amount,
    
    CASE WHEN COALESCE(ARRAY_SIZE(p.transaction_json:items), 0) = 0 
         THEN TRUE ELSE FALSE END AS has_empty_items,
    
    -- Raw JSON for debugging
    p.transaction_json AS raw_transaction_json

FROM parsed_json p;

-- ============================================================================
-- VIEW: Flatten Items from ClientC Transactions
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_client_c_transaction_items AS
SELECT
    t.raw_record_id,
    t.source_file_name,
    t.source_client,
    t.transaction_id,
    t.transaction_id_raw,
    t.order_id,
    t.customer_id,
    t.order_date,
    t.ingestion_timestamp,
    t.batch_id,
    
    -- Item Position
    item.index AS item_index,
    item.index + 1 AS line_number,
    
    -- ================================================================
    -- RAW ITEM FIELDS
    -- ================================================================
    item.value:sku::VARCHAR AS sku_raw,
    item.value:product_id::VARCHAR AS product_id_raw,
    item.value:description::VARCHAR AS description_raw,
    item.value:name::VARCHAR AS name_raw,
    item.value:quantity::VARCHAR AS quantity_raw,
    item.value:qty::VARCHAR AS qty_alt_raw,
    item.value:unit_price::VARCHAR AS unit_price_raw,
    item.value:price::VARCHAR AS price_alt_raw,
    item.value:currency::VARCHAR AS currency_raw,
    
    -- Full item for metadata
    item.value AS item_metadata_json,
    
    -- ================================================================
    -- NORMALIZED FIELDS
    -- ================================================================
    UPPER(TRIM(NULLIF(COALESCE(
        item.value:sku::VARCHAR, 
        item.value:product_id::VARCHAR
    ), ''))) AS sku,
    
    TRIM(NULLIF(COALESCE(
        item.value:description::VARCHAR, 
        item.value:name::VARCHAR
    ), '')) AS description,
    
    TRY_TO_INTEGER(COALESCE(
        item.value:quantity::VARCHAR, 
        item.value:qty::VARCHAR
    )) AS quantity,
    
    TRY_TO_DECIMAL(COALESCE(
        item.value:unit_price::VARCHAR, 
        item.value:price::VARCHAR
    ), 18, 4) AS unit_price,
    
    COALESCE(UPPER(TRIM(NULLIF(item.value:currency::VARCHAR, ''))), 'USD') AS currency,
    
    -- Absolute Values
    ABS(TRY_TO_INTEGER(COALESCE(
        item.value:quantity::VARCHAR, 
        item.value:qty::VARCHAR
    ))) AS quantity_abs,
    
    ABS(TRY_TO_DECIMAL(COALESCE(
        item.value:unit_price::VARCHAR, 
        item.value:price::VARCHAR
    ), 18, 4)) AS unit_price_abs,
    
    -- Line Totals
    TRY_TO_INTEGER(COALESCE(item.value:quantity::VARCHAR, item.value:qty::VARCHAR)) * 
    TRY_TO_DECIMAL(COALESCE(item.value:unit_price::VARCHAR, item.value:price::VARCHAR), 18, 4) AS line_total,
    
    ABS(TRY_TO_INTEGER(COALESCE(item.value:quantity::VARCHAR, item.value:qty::VARCHAR))) * 
    ABS(TRY_TO_DECIMAL(COALESCE(item.value:unit_price::VARCHAR, item.value:price::VARCHAR), 18, 4)) AS line_total_abs,
    
    -- ================================================================
    -- DATA QUALITY FLAGS
    -- ================================================================
    CASE WHEN NULLIF(TRIM(COALESCE(
        item.value:sku::VARCHAR, 
        item.value:product_id::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_sku,
    
    CASE WHEN NULLIF(TRIM(COALESCE(
        item.value:description::VARCHAR, 
        item.value:name::VARCHAR
    )), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_description,
    
    CASE WHEN TRY_TO_INTEGER(COALESCE(
        item.value:quantity::VARCHAR, 
        item.value:qty::VARCHAR
    )) IS NULL THEN TRUE ELSE FALSE END AS is_missing_quantity,
    
    CASE WHEN TRY_TO_INTEGER(COALESCE(
        item.value:quantity::VARCHAR, 
        item.value:qty::VARCHAR
    )) < 0 THEN TRUE ELSE FALSE END AS is_negative_quantity,
    
    CASE WHEN TRY_TO_DECIMAL(COALESCE(
        item.value:unit_price::VARCHAR, 
        item.value:price::VARCHAR
    ), 18, 4) < 0 THEN TRUE ELSE FALSE END AS is_negative_unit_price

FROM staging.v_stg_client_c_transactions t,
LATERAL FLATTEN(INPUT => t.items_json, OUTER => TRUE) item
WHERE item.value IS NOT NULL;

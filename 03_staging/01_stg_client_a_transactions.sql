/*
================================================================================
STAGING: CLIENT A TRANSACTIONS (XML) - SNOWFLAKE COMPATIBLE
================================================================================
Purpose: Parse and flatten XML transactions into structured format
Author:  Data Platform Team
Version: 3.0 (Snowflake XML Syntax Validated)

SNOWFLAKE XML PARSING NOTES:
- XMLGET(xml, 'tagname') returns the first matching child element
- To get text content: XMLGET(xml, 'tag'):"$"::VARCHAR
- To get attribute: XMLGET(xml, 'tag'):"@attrname"::VARCHAR
- For nested elements, chain XMLGET calls
- Use LATERAL FLATTEN with XMLGET for repeated elements
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA staging;

-- ============================================================================
-- VIEW: Parse XML Transactions from ClientA
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_client_a_transactions AS
WITH xml_sources AS (
    -- Source 1: Standard XML files
    SELECT
        r.raw_record_id,
        r.source_file_name,
        r.source_client,
        r.source_format,
        r.ingestion_timestamp,
        r.batch_id,
        r.record_hash AS file_hash,
        r.raw_xml_content AS xml_doc
    FROM raw.raw_xml_transactions r
    WHERE r.source_client = 'ClientA'
    
    UNION ALL
    
    -- Source 2: Parsed TXT-XML files
    SELECT
        r.raw_record_id,
        r.source_file_name,
        r.source_client,
        'XML_FROM_TXT' AS source_format,
        r.ingestion_timestamp,
        r.batch_id,
        r.record_hash AS file_hash,
        r.raw_xml_content AS xml_doc
    FROM raw.raw_txt_xml_transactions r
    WHERE r.xml_parse_success = TRUE
),

-- Flatten to transaction level
-- XMLGET returns child elements; we flatten on the array of Transaction nodes
xml_transactions AS (
    SELECT
        xs.raw_record_id,
        xs.source_file_name,
        xs.source_client,
        xs.source_format,
        xs.ingestion_timestamp,
        xs.batch_id,
        xs.file_hash,
        txn.value AS transaction_xml,
        txn.index AS transaction_index
    FROM xml_sources xs,
    LATERAL FLATTEN(
        INPUT => xs.xml_doc:"$", 
        OUTER => TRUE
    ) txn
    WHERE txn.value:"@" = 'Transaction'
       OR GET(txn.value, '@') = 'Transaction'
)

SELECT
    -- ================================================================
    -- IDENTIFIERS
    -- ================================================================
    x.raw_record_id,
    x.source_file_name,
    x.source_client,
    x.source_format,
    x.ingestion_timestamp,
    x.batch_id,
    x.file_hash,
    x.transaction_index,
    
    -- ================================================================
    -- RAW EXTRACTED FIELDS
    -- Snowflake XML: XMLGET(parent, 'child'):"$" gets text content
    -- ================================================================
    
    -- Transaction ID
    XMLGET(x.transaction_xml, 'TransactionID'):"$"::VARCHAR AS transaction_id_raw,
    
    -- Order Level
    XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderID'):"$"::VARCHAR AS order_id_raw,
    XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderDate'):"$"::VARCHAR AS order_date_raw,
    
    -- Customer Level (nested 3 levels)
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'CustomerID'):"$"::VARCHAR AS customer_id_raw,
    XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$"::VARCHAR AS first_name_raw,
    XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'LastName'):"$"::VARCHAR AS last_name_raw,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Email'):"$"::VARCHAR AS email_raw,
    
    -- Payment Level
    XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR AS payment_method_raw,
    XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"$"::VARCHAR AS payment_amount_raw,
    XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"@currency"::VARCHAR AS payment_currency_raw,
    
    -- Items container (for later flattening)
    XMLGET(x.transaction_xml, 'Items') AS items_xml,
    
    -- ================================================================
    -- NORMALIZED FIELDS
    -- ================================================================
    
    -- Transaction ID - normalized
    UPPER(TRIM(NULLIF(
        XMLGET(x.transaction_xml, 'TransactionID'):"$"::VARCHAR, 
    ''))) AS transaction_id,
    
    -- Order ID & Date - normalized
    UPPER(TRIM(NULLIF(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderID'):"$"::VARCHAR, 
    ''))) AS order_id,
    
    TRY_TO_DATE(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderDate'):"$"::VARCHAR
    ) AS order_date,
    
    -- Customer Fields - normalized
    UPPER(TRIM(NULLIF(
        XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'CustomerID'):"$"::VARCHAR, 
    ''))) AS customer_id,
    
    INITCAP(TRIM(NULLIF(
        XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$"::VARCHAR, 
    ''))) AS first_name,
    
    INITCAP(TRIM(NULLIF(
        XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'LastName'):"$"::VARCHAR, 
    ''))) AS last_name,
    
    LOWER(TRIM(NULLIF(
        XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Email'):"$"::VARCHAR, 
    ''))) AS email,
    
    -- Email Validation
    CASE 
        WHEN NULLIF(TRIM(
            XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Email'):"$"::VARCHAR
        ), '') IS NULL THEN 'MISSING'
        WHEN NOT REGEXP_LIKE(
            LOWER(TRIM(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Email'):"$"::VARCHAR)),
            '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        ) THEN 'INVALID'
        ELSE 'VALID'
    END AS email_validation_status,
    
    -- Payment Method Normalization
    CASE 
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR, 
        '')), ' ', '')) IN ('CREDITCARD', 'CREDIT_CARD', 'CC') THEN 'CREDIT_CARD'
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR, 
        '')), ' ', '')) IN ('DEBITCARD', 'DEBIT_CARD', 'DC') THEN 'DEBIT_CARD'
        WHEN UPPER(TRIM(COALESCE(
            XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR, 
        ''))) = 'PAYPAL' THEN 'PAYPAL'
        WHEN UPPER(REPLACE(TRIM(COALESCE(
            XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR, 
        '')), ' ', '')) IN ('BANKTRANSFER', 'BANK_TRANSFER', 'WIRE') THEN 'BANK_TRANSFER'
        WHEN NULLIF(TRIM(
            XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR
        ), '') IS NULL THEN 'MISSING'
        ELSE 'OTHER'
    END AS payment_method,
    
    -- Payment Amount
    TRY_TO_DECIMAL(
        XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"$"::VARCHAR, 
        18, 4
    ) AS payment_amount,
    
    ABS(TRY_TO_DECIMAL(
        XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"$"::VARCHAR, 
        18, 4
    )) AS payment_amount_abs,
    
    COALESCE(
        UPPER(TRIM(XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"@currency"::VARCHAR)), 
        'USD'
    ) AS payment_currency,
    
    -- ================================================================
    -- METADATA EXTRACTION (Unexpected/Extra Fields)
    -- These preserve any additional nested structures
    -- ================================================================
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'LoyaltyTier'):"$"::VARCHAR AS loyalty_tier_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Metadata') AS metadata_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Tags') AS tags_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Notes') AS notes_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Preferences') AS preferences_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Flags') AS flags_extra,
    XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Attributes') AS attributes_extra,
    XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Fees') AS fees_extra,
    
    -- Summary of extra fields detected
    OBJECT_CONSTRUCT_KEEP_NULL(
        'loyalty_tier', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'LoyaltyTier'):"$"::VARCHAR,
        'has_metadata', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Metadata') IS NOT NULL,
        'has_tags', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Tags') IS NOT NULL,
        'has_preferences', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Preferences') IS NOT NULL,
        'has_notes', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Notes') IS NOT NULL,
        'has_flags', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Flags') IS NOT NULL,
        'has_attributes', XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Attributes') IS NOT NULL,
        'has_fees', XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Fees') IS NOT NULL
    ) AS extra_fields_summary,
    
    -- ================================================================
    -- DATA QUALITY FLAGS
    -- ================================================================
    
    -- Missing critical fields
    CASE WHEN NULLIF(TRIM(
        XMLGET(x.transaction_xml, 'TransactionID'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_transaction_id,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderID'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_order_id,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderDate'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_order_date,
    
    CASE WHEN TRY_TO_DATE(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderDate'):"$"::VARCHAR
    ) IS NULL 
    AND NULLIF(TRIM(
        XMLGET(XMLGET(x.transaction_xml, 'Order'), 'OrderDate'):"$"::VARCHAR
    ), '') IS NOT NULL THEN TRUE ELSE FALSE END AS is_invalid_order_date,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'CustomerID'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_customer_id,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Email'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_email,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_first_name,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(XMLGET(XMLGET(x.transaction_xml, 'Order'), 'Customer'), 'Name'), 'LastName'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_last_name,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Method'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment_method,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment_amount,
    
    -- Value anomalies
    CASE WHEN TRY_TO_DECIMAL(
        XMLGET(XMLGET(x.transaction_xml, 'Payment'), 'Amount'):"$"::VARCHAR, 18, 4
    ) < 0 THEN TRUE ELSE FALSE END AS is_negative_payment_amount,
    
    -- Raw XML for debugging
    x.transaction_xml AS raw_transaction_xml

FROM xml_transactions x;

-- ============================================================================
-- VIEW: Flatten Items from ClientA Transactions
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_client_a_transaction_items AS
WITH items_flattened AS (
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
        item.value AS item_xml,
        item.index AS item_index
    FROM staging.v_stg_client_a_transactions t,
    LATERAL FLATTEN(
        INPUT => t.items_xml:"$",
        OUTER => TRUE
    ) item
    WHERE item.value:"@" = 'Item'
       OR GET(item.value, '@') = 'Item'
       OR item.value IS NOT NULL
)

SELECT
    f.raw_record_id,
    f.source_file_name,
    f.source_client,
    f.transaction_id,
    f.transaction_id_raw,
    f.order_id,
    f.customer_id,
    f.order_date,
    f.ingestion_timestamp,
    f.batch_id,
    
    -- Item Position
    f.item_index,
    f.item_index + 1 AS line_number,
    
    -- ================================================================
    -- RAW ITEM FIELDS
    -- ================================================================
    XMLGET(f.item_xml, 'SKU'):"$"::VARCHAR AS sku_raw,
    XMLGET(f.item_xml, 'Description'):"$"::VARCHAR AS description_raw,
    XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR AS quantity_raw,
    XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR AS unit_price_raw,
    XMLGET(f.item_xml, 'UnitPrice'):"@currency"::VARCHAR AS currency_raw,
    
    -- ================================================================
    -- ITEM METADATA (Extra Fields)
    -- ================================================================
    XMLGET(f.item_xml, 'Attributes') AS attributes_extra,
    XMLGET(f.item_xml, 'Warranty') AS warranty_extra,
    XMLGET(f.item_xml, 'GiftOptions') AS gift_options_extra,
    
    OBJECT_CONSTRUCT_KEEP_NULL(
        'has_attributes', XMLGET(f.item_xml, 'Attributes') IS NOT NULL,
        'has_warranty', XMLGET(f.item_xml, 'Warranty') IS NOT NULL,
        'has_gift_options', XMLGET(f.item_xml, 'GiftOptions') IS NOT NULL
    ) AS item_extra_fields_summary,
    
    -- ================================================================
    -- NORMALIZED FIELDS
    -- ================================================================
    UPPER(TRIM(NULLIF(
        XMLGET(f.item_xml, 'SKU'):"$"::VARCHAR, 
    ''))) AS sku,
    
    TRIM(NULLIF(
        XMLGET(f.item_xml, 'Description'):"$"::VARCHAR, 
    '')) AS description,
    
    TRY_TO_INTEGER(
        XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR
    ) AS quantity,
    
    TRY_TO_DECIMAL(
        XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 
        18, 4
    ) AS unit_price,
    
    COALESCE(
        UPPER(TRIM(XMLGET(f.item_xml, 'UnitPrice'):"@currency"::VARCHAR)), 
        'USD'
    ) AS currency,
    
    -- Absolute Values
    ABS(TRY_TO_INTEGER(
        XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR
    )) AS quantity_abs,
    
    ABS(TRY_TO_DECIMAL(
        XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 
        18, 4
    )) AS unit_price_abs,
    
    -- Line Total Calculations
    TRY_TO_INTEGER(XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR) * 
    TRY_TO_DECIMAL(XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 18, 4) AS line_total,
    
    ABS(TRY_TO_INTEGER(XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR)) * 
    ABS(TRY_TO_DECIMAL(XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 18, 4)) AS line_total_abs,
    
    -- ================================================================
    -- DATA QUALITY FLAGS
    -- ================================================================
    CASE WHEN NULLIF(TRIM(
        XMLGET(f.item_xml, 'SKU'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_sku,
    
    CASE WHEN NULLIF(TRIM(
        XMLGET(f.item_xml, 'Description'):"$"::VARCHAR
    ), '') IS NULL THEN TRUE ELSE FALSE END AS is_missing_description,
    
    CASE WHEN TRY_TO_INTEGER(
        XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR
    ) IS NULL THEN TRUE ELSE FALSE END AS is_missing_quantity,
    
    CASE WHEN TRY_TO_INTEGER(
        XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR
    ) < 0 THEN TRUE ELSE FALSE END AS is_negative_quantity,
    
    CASE WHEN TRY_TO_INTEGER(
        XMLGET(f.item_xml, 'Quantity'):"$"::VARCHAR
    ) = 0 THEN TRUE ELSE FALSE END AS is_zero_quantity,
    
    CASE WHEN TRY_TO_DECIMAL(
        XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 18, 4
    ) IS NULL THEN TRUE ELSE FALSE END AS is_missing_unit_price,
    
    CASE WHEN TRY_TO_DECIMAL(
        XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 18, 4
    ) < 0 THEN TRUE ELSE FALSE END AS is_negative_unit_price,
    
    CASE WHEN TRY_TO_DECIMAL(
        XMLGET(f.item_xml, 'UnitPrice'):"$"::VARCHAR, 18, 4
    ) = 0 THEN TRUE ELSE FALSE END AS is_zero_unit_price,
    
    -- Raw XML
    f.item_xml AS raw_item_xml

FROM items_flattened f
WHERE f.item_xml IS NOT NULL;
/*
================================================================================
RAW INGESTION: COPY INTO STATEMENTS
================================================================================
Purpose: Load all source files into raw tables
Author:  Data Platform Team
Version: 2.0

Prerequisites:
1. Run scripts 01-04 first
2. Upload files to stages:
   PUT file:///path/to/ClientA_Transactions_1.xml @raw.stg_xml_files;
   PUT file:///path/to/transactions.json @raw.stg_json_files;
   PUT file:///path/to/Customer.csv @raw.stg_csv_files;
   PUT file:///path/to/ClientA_Transactions_4.txt @raw.stg_txt_files;
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;
USE WAREHOUSE financial_etl_wh;

-- ============================================================================
-- GENERATE BATCH ID
-- ============================================================================
SET batch_id = (SELECT 'BATCH_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS_FF3'));
SET ingestion_start = (SELECT CURRENT_TIMESTAMP());

SELECT $batch_id AS current_batch_id;

-- ============================================================================
-- SECTION 1: LOAD XML TRANSACTION FILES (ClientA)
-- ============================================================================

-- Pattern-based load for all XML files at once
COPY INTO raw.raw_xml_transactions (
    raw_xml_content,
    source_file_name,
    source_file_path,  -- ADD THIS
    source_client,
    source_format,
    batch_id,
    record_hash
)
FROM (
    SELECT 
        $1 AS raw_xml_content,
        METADATA$FILENAME AS source_file_name,
        METADATA$FILE_ROW_NUMBER AS source_file_path,  -- Or use stage path
        'ClientA' AS source_client,
        'XML' AS source_format,
        $batch_id AS batch_id,
        SHA2(TO_VARCHAR($1), 256) AS record_hash
    FROM @raw.stg_xml_files
)
FILE_FORMAT = raw.ff_xml
PATTERN = '.*[Cc]lient[Aa].*\\.xml'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- Log XML ingestion
INSERT INTO raw.ingestion_audit_log (
    batch_id, source_file_pattern, target_table, ingestion_start_time, 
    ingestion_end_time, rows_loaded, status
)
SELECT 
    $batch_id,
    '.*ClientA.*\\.xml',
    'raw.raw_xml_transactions',
    $ingestion_start,
    CURRENT_TIMESTAMP(),
    COUNT(*),
    'SUCCESS'
FROM raw.raw_xml_transactions 
WHERE batch_id = $batch_id;

-- ============================================================================
-- SECTION 2: LOAD TXT FILE WITH XML CONTENT
-- ============================================================================

-- Step 2a: Load raw text content
COPY INTO raw.raw_txt_xml_transactions (
    raw_text_content,
    source_file_name,
    source_client,
    source_format,
    batch_id
)
FROM (
    SELECT 
        $1 AS raw_text_content,
        METADATA$FILENAME AS source_file_name,
        'ClientA' AS source_client,
        'TXT_XML' AS source_format,
        $batch_id AS batch_id
    FROM @raw.stg_txt_files
)
FILE_FORMAT = raw.ff_txt_raw
PATTERN = '.*\\.txt'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- Step 2b: Parse XML from text content
-- The TXT file contains XML without root element, so we wrap it
UPDATE raw.raw_txt_xml_transactions
SET 
    raw_xml_content = TRY_PARSE_XML(
        '<?xml version="1.0" encoding="UTF-8"?><SalesData client="ClientA">' || 
        REGEXP_REPLACE(
            REGEXP_REPLACE(raw_text_content, '<!--[^>]*-->', ''),  -- Remove XML comments
            '^[^<]*', ''  -- Remove any text before first tag
        ) ||
        '</SalesData>'
    ),
    xml_parse_success = CASE 
        WHEN TRY_PARSE_XML(
            '<SalesData>' || 
            REGEXP_REPLACE(REGEXP_REPLACE(raw_text_content, '<!--[^>]*-->', ''), '^[^<]*', '') ||
            '</SalesData>'
        ) IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END,
    xml_parse_error = CASE 
        WHEN TRY_PARSE_XML(
            '<SalesData>' || 
            REGEXP_REPLACE(REGEXP_REPLACE(raw_text_content, '<!--[^>]*-->', ''), '^[^<]*', '') ||
            '</SalesData>'
        ) IS NULL THEN 'Failed to parse XML from text content'
        ELSE NULL
    END,
    record_hash = SHA2(raw_text_content, 256),
    updated_at = CURRENT_TIMESTAMP()
WHERE batch_id = $batch_id
  AND (raw_xml_content IS NULL OR updated_at IS NULL);

-- ============================================================================
-- SECTION 3: LOAD JSON TRANSACTION FILE (ClientC)
-- ============================================================================

COPY INTO raw.raw_json_transactions (
    raw_json_content,
    source_file_name,
    source_client,
    source_format,
    batch_id,
    record_hash
)
FROM (
    SELECT 
        $1 AS raw_json_content,
        METADATA$FILENAME AS source_file_name,
        'ClientC' AS source_client,
        'JSON' AS source_format,
        $batch_id AS batch_id,
        SHA2(TO_VARCHAR($1), 256) AS record_hash
    FROM @raw.stg_json_files
)
FILE_FORMAT = raw.ff_json
PATTERN = '.*\\.json'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- ============================================================================
-- SECTION 4: LOAD CSV REFERENCE FILES
-- ============================================================================

-- 4a: Customers (handles Customer.csv, Customer.CSV)
COPY INTO raw.raw_csv_customers (
    customer_id_raw, first_name_raw, last_name_raw, email_raw, 
    loyalty_tier_raw, signup_source_raw, is_active_raw,
    source_file_name, batch_id, file_row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7,
        METADATA$FILENAME,
        $batch_id,
        METADATA$FILE_ROW_NUMBER
    FROM @raw.stg_csv_files
)
FILE_FORMAT = raw.ff_csv_standard
PATTERN = '.*[Cc]ustomer.*\\.[Cc][Ss][Vv]'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- 4b: Orders (handles Orders.csv, Order.csv)
COPY INTO raw.raw_csv_orders (
    order_id_raw, customer_id_raw, order_date_raw, 
    status_raw, total_amount_raw, currency_raw,
    source_file_name, batch_id, file_row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6,
        METADATA$FILENAME,
        $batch_id,
        METADATA$FILE_ROW_NUMBER
    FROM @raw.stg_csv_files
)
FILE_FORMAT = raw.ff_csv_standard
PATTERN = '.*[Oo]rder[s]?\\.[Cc][Ss][Vv]'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- 4c: Products (handles Products.csv, Product.csv)
COPY INTO raw.raw_csv_products (
    product_id_raw, sku_raw, product_name_raw, description_raw, 
    category_raw, price_raw, currency_raw,
    source_file_name, batch_id, file_row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7,
        METADATA$FILENAME,
        $batch_id,
        METADATA$FILE_ROW_NUMBER
    FROM @raw.stg_csv_files
)
FILE_FORMAT = raw.ff_csv_standard
PATTERN = '.*[Pp]roduct[s]?\\.[Cc][Ss][Vv]'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- 4d: Payments
COPY INTO raw.raw_csv_payments (
    payment_id_raw, order_id_raw, transaction_id_raw, payment_method_raw, 
    amount_raw, currency_raw, payment_date_raw, status_raw,
    source_file_name, batch_id, file_row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME,
        $batch_id,
        METADATA$FILE_ROW_NUMBER
    FROM @raw.stg_csv_files
)
FILE_FORMAT = raw.ff_csv_standard
PATTERN = '.*[Pp]ayment[s]?\\.[Cc][Ss][Vv]'
ON_ERROR = 'CONTINUE'
FORCE = FALSE
PURGE = FALSE;

-- ============================================================================
-- SECTION 5: GENERATE RECORD HASHES FOR CSV
-- ============================================================================

UPDATE raw.raw_csv_customers
SET record_hash = SHA2(
    COALESCE(customer_id_raw, '') || '|' ||
    COALESCE(first_name_raw, '') || '|' ||
    COALESCE(last_name_raw, '') || '|' ||
    COALESCE(email_raw, '') || '|' ||
    COALESCE(loyalty_tier_raw, ''),
    256
)
WHERE batch_id = $batch_id AND record_hash IS NULL;

UPDATE raw.raw_csv_orders
SET record_hash = SHA2(
    COALESCE(order_id_raw, '') || '|' ||
    COALESCE(customer_id_raw, '') || '|' ||
    COALESCE(order_date_raw, '') || '|' ||
    COALESCE(total_amount_raw, ''),
    256
)
WHERE batch_id = $batch_id AND record_hash IS NULL;

UPDATE raw.raw_csv_products
SET record_hash = SHA2(
    COALESCE(product_id_raw, '') || '|' ||
    COALESCE(sku_raw, '') || '|' ||
    COALESCE(product_name_raw, '') || '|' ||
    COALESCE(price_raw, ''),
    256
)
WHERE batch_id = $batch_id AND record_hash IS NULL;

UPDATE raw.raw_csv_payments
SET record_hash = SHA2(
    COALESCE(payment_id_raw, '') || '|' ||
    COALESCE(order_id_raw, '') || '|' ||
    COALESCE(amount_raw, '') || '|' ||
    COALESCE(payment_method_raw, ''),
    256
)
WHERE batch_id = $batch_id AND record_hash IS NULL;

-- ============================================================================
-- SECTION 6: INGESTION SUMMARY
-- ============================================================================

SELECT '==================== INGESTION SUMMARY ====================' AS report_header;

SELECT 
    'raw_xml_transactions' AS table_name, 
    COUNT(*) AS total_records,
    COUNT(DISTINCT source_file_name) AS files_loaded
FROM raw.raw_xml_transactions 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_txt_xml_transactions', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_txt_xml_transactions 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_json_transactions', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_json_transactions 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_csv_customers', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_csv_customers 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_csv_orders', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_csv_orders 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_csv_products', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_csv_products 
WHERE batch_id = $batch_id

UNION ALL

SELECT 
    'raw_csv_payments', 
    COUNT(*),
    COUNT(DISTINCT source_file_name)
FROM raw.raw_csv_payments 
WHERE batch_id = $batch_id;

-- TXT-XML Parse Status
SELECT 
    'TXT XML Parse Results' AS check_type,
    SUM(CASE WHEN xml_parse_success = TRUE THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN xml_parse_success = FALSE THEN 1 ELSE 0 END) AS failure_count
FROM raw.raw_txt_xml_transactions 
WHERE batch_id = $batch_id;

SELECT 'Batch ' || $batch_id || ' completed at ' || CURRENT_TIMESTAMP() AS completion_message;

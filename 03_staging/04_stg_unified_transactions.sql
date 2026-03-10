/*
================================================================================
STAGING: UNIFIED TRANSACTIONS VIEW
================================================================================
Purpose: Combine transactions from all clients into single unified structure
Author:  Data Platform Team
Version: 1.0

This view provides a single interface to all transaction data regardless
of source format (XML, JSON, CSV).
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA staging;

-- ============================================================================
-- UNIFIED TRANSACTION HEADER VIEW
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_unified_transactions AS

-- ClientA XML Transactions
SELECT
    'ClientA' AS source_client,
    source_format,
    source_file_name,
    raw_record_id,
    batch_id,
    ingestion_timestamp,
    
    -- Business Keys
    transaction_id,
    transaction_id_raw,
    order_id,
    customer_id,
    order_date,
    
    -- Customer Info
    first_name,
    last_name,
    email,
    email_validation_status,
    loyalty_tier_extra AS loyalty_tier,
    
    -- Payment Info
    payment_method,
    payment_amount,
    payment_amount_abs,
    payment_currency,
    
    -- DQ Flags
    is_missing_transaction_id,
    is_missing_order_id,
    is_missing_order_date,
    is_missing_customer_id,
    is_missing_email,
    is_missing_payment_method,
    is_missing_payment_amount,
    is_negative_payment_amount,
    
    -- Combined DQ Status
    CASE 
        WHEN is_missing_transaction_id THEN 'CRITICAL'
        WHEN is_missing_order_id OR is_missing_customer_id OR is_negative_payment_amount THEN 'INVALID'
        WHEN is_missing_email OR is_missing_order_date THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    
    -- Metadata
    extra_fields_summary AS metadata_json,
    raw_transaction_xml AS raw_payload

FROM staging.v_stg_client_a_transactions

UNION ALL

-- ClientC JSON Transactions
SELECT
    'ClientC' AS source_client,
    source_format,
    source_file_name,
    raw_record_id,
    batch_id,
    ingestion_timestamp,
    
    -- Business Keys
    transaction_id,
    transaction_id_raw,
    order_id,
    customer_id,
    order_date,
    
    -- Customer Info
    first_name,
    last_name,
    email,
    email_validation_status,
    loyalty_tier,
    
    -- Payment Info
    payment_method,
    payment_amount,
    payment_amount_abs,
    payment_currency,
    
    -- DQ Flags
    is_missing_transaction_id,
    is_missing_order_id,
    is_missing_order_date,
    is_missing_customer_id,
    is_missing_email,
    is_missing_payment_method,
    is_missing_payment_amount,
    is_negative_payment_amount,
    
    -- Combined DQ Status
    CASE 
        WHEN is_missing_transaction_id THEN 'CRITICAL'
        WHEN is_missing_order_id OR is_missing_customer_id OR is_negative_payment_amount THEN 'INVALID'
        WHEN is_missing_email OR is_missing_order_date THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    
    -- Metadata
    transaction_metadata AS metadata_json,
    raw_transaction_json AS raw_payload

FROM staging.v_stg_client_c_transactions;

-- ============================================================================
-- UNIFIED TRANSACTION ITEMS VIEW
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_stg_unified_transaction_items AS

-- ClientA Items
SELECT
    'ClientA' AS source_client,
    source_file_name,
    raw_record_id,
    batch_id,
    ingestion_timestamp,
    
    -- Keys
    transaction_id,
    order_id,
    customer_id,
    order_date,
    line_number,
    
    -- Item Info
    sku,
    sku_raw,
    description,
    quantity,
    quantity_abs,
    unit_price,
    unit_price_abs,
    currency,
    line_total,
    line_total_abs,
    
    -- DQ Flags
    is_missing_sku,
    is_missing_description,
    is_negative_quantity,
    is_negative_unit_price,
    
    -- Combined DQ Status
    CASE 
        WHEN is_missing_sku THEN 'INVALID'
        WHEN is_negative_quantity OR is_negative_unit_price THEN 'WARNING'
        WHEN is_missing_description THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    
    -- Metadata
    item_extra_fields_summary AS item_metadata

FROM staging.v_stg_client_a_transaction_items

UNION ALL

-- ClientC Items
SELECT
    'ClientC' AS source_client,
    source_file_name,
    raw_record_id,
    batch_id,
    ingestion_timestamp,
    
    -- Keys
    transaction_id,
    order_id,
    customer_id,
    order_date,
    line_number,
    
    -- Item Info
    sku,
    sku_raw,
    description,
    quantity,
    quantity_abs,
    unit_price,
    unit_price_abs,
    currency,
    line_total,
    line_total_abs,
    
    -- DQ Flags
    is_missing_sku,
    is_missing_description,
    is_negative_quantity,
    is_negative_unit_price,
    
    -- Combined DQ Status
    CASE 
        WHEN is_missing_sku THEN 'INVALID'
        WHEN is_negative_quantity OR is_negative_unit_price THEN 'WARNING'
        WHEN is_missing_description THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    
    -- Metadata
    item_metadata_json AS item_metadata

FROM staging.v_stg_client_c_transaction_items;

-- ============================================================================
-- STAGING TABLES (Materialized for performance)
-- ============================================================================

-- Materialized staging table for transactions
CREATE OR REPLACE TABLE staging.stg_transactions AS
SELECT * FROM staging.v_stg_unified_transactions;

-- Materialized staging table for items
CREATE OR REPLACE TABLE staging.stg_transaction_items AS
SELECT * FROM staging.v_stg_unified_transaction_items;

-- Materialized staging table for customers
CREATE OR REPLACE TABLE staging.stg_customers AS
SELECT * FROM staging.v_stg_customers;

-- Materialized staging table for orders
CREATE OR REPLACE TABLE staging.stg_orders AS
SELECT * FROM staging.v_stg_orders;

-- Materialized staging table for products
CREATE OR REPLACE TABLE staging.stg_products AS
SELECT * FROM staging.v_stg_products;

-- Materialized staging table for payments
CREATE OR REPLACE TABLE staging.stg_payments AS
SELECT * FROM staging.v_stg_payments;

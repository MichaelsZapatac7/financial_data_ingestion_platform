/*
================================================================================
DATA QUALITY: REPORTING VIEWS
================================================================================
Purpose: Create views for data quality monitoring and reporting
Author:  Data Platform Team
Version: 2.0
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA data_quality;

-- ============================================================================
-- ISSUE SUMMARY BY CATEGORY
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_dq_issue_summary AS
SELECT
    batch_id,
    source_client,
    issue_category,
    issue_type,
    issue_severity,
    COUNT(*) AS issue_count,
    COUNT(DISTINCT source_file_name) AS affected_files,
    COUNT(DISTINCT record_identifier) AS affected_records,
    MIN(detected_at) AS first_detected,
    MAX(detected_at) AS last_detected,
    SUM(CASE WHEN is_resolved THEN 1 ELSE 0 END) AS resolved_count
FROM data_quality.dq_issue_log
GROUP BY batch_id, source_client, issue_category, issue_type, issue_severity
ORDER BY issue_count DESC;

-- ============================================================================
-- DUPLICATE SUMMARY
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_dq_duplicate_summary AS
SELECT
    batch_id,
    source_client,
    duplicate_type,
    COUNT(*) AS duplicate_groups,
    SUM(occurrence_count) AS total_duplicate_records,
    SUM(occurrence_count) - COUNT(*) AS excess_records,
    MIN(detected_at) AS first_detected,
    MAX(detected_at) AS last_detected
FROM data_quality.dq_duplicate_tracker
GROUP BY batch_id, source_client, duplicate_type;

-- ============================================================================
-- TRANSACTION QUALITY DASHBOARD
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_transaction_quality_dashboard AS
SELECT
    source_client,
    dq_status,
    COUNT(*) AS transaction_count,
    SUM(payment_amount_abs) AS total_amount,
    AVG(payment_amount_abs) AS avg_amount,
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date,
    SUM(CASE WHEN has_dq_issues THEN 1 ELSE 0 END) AS issues_flagged,
    SUM(dq_issue_count) AS total_issue_count
FROM canonical.fact_transaction
GROUP BY source_client, dq_status
ORDER BY source_client, dq_status;

-- ============================================================================
-- ANOMALY DETAIL REPORT
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_anomaly_detail_report AS
SELECT
    ft.transaction_id,
    ft.order_id,
    ft.customer_id,
    ft.source_client,
    ft.source_file_name,
    ft.order_date,
    ft.dq_status,
    ft.dq_issues,
    ft.dq_issue_count,
    ft.payment_amount,
    ft.payment_amount_abs,
    CASE WHEN ft.payment_amount < 0 THEN TRUE ELSE FALSE END AS has_negative_payment,
    ft.has_amount_variance,
    ft.amount_variance,
    ft.item_count,
    (SELECT COUNT(*) FROM canonical.fact_transaction_item i 
     WHERE i.transaction_sk = ft.transaction_sk AND i.has_negative_quantity = TRUE) AS negative_quantity_lines,
    (SELECT COUNT(*) FROM canonical.fact_transaction_item i 
     WHERE i.transaction_sk = ft.transaction_sk AND i.has_negative_price = TRUE) AS negative_price_lines,
    (SELECT COUNT(*) FROM canonical.fact_transaction_item i 
     WHERE i.transaction_sk = ft.transaction_sk AND i.is_missing_sku = TRUE) AS missing_sku_lines,
    ft.ingestion_timestamp,
    ft.created_at
FROM canonical.fact_transaction ft
WHERE ft.has_dq_issues = TRUE
   OR ft.dq_status != 'VALID'
   OR ft.has_amount_variance = TRUE
ORDER BY ft.dq_issue_count DESC, ft.order_date DESC;

-- ============================================================================
-- FILE QUALITY SUMMARY
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_file_quality_summary AS
SELECT
    source_file_name,
    source_client,
    source_format,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN dq_status = 'VALID' THEN 1 ELSE 0 END) AS valid_count,
    SUM(CASE WHEN dq_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
    SUM(CASE WHEN dq_status = 'INVALID' THEN 1 ELSE 0 END) AS invalid_count,
    SUM(CASE WHEN dq_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    ROUND(100.0 * SUM(CASE WHEN dq_status = 'VALID' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS valid_percentage,
    ROUND(100.0 * SUM(CASE WHEN dq_status IN ('INVALID', 'CRITICAL') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS error_percentage,
    SUM(payment_amount_abs) AS total_amount,
    SUM(item_count) AS total_items,
    MIN(ingestion_timestamp) AS first_ingested,
    MAX(ingestion_timestamp) AS last_ingested
FROM canonical.fact_transaction
GROUP BY source_file_name, source_client, source_format
ORDER BY source_file_name;

-- ============================================================================
-- BATCH QUALITY SUMMARY
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_batch_quality_summary AS
SELECT
    m.batch_id,
    m.metric_date,
    m.source_client,
    m.total_records_received,
    m.total_records_loaded,
    m.total_records_rejected,
    m.total_records_warning,
    m.missing_field_count,
    m.invalid_format_count,
    m.negative_value_count,
    m.duplicate_count,
    m.rejection_rate,
    m.warning_rate,
    m.data_quality_score,
    m.calculated_at,
    (SELECT COUNT(*) FROM data_quality.dq_issue_log WHERE batch_id = m.batch_id AND source_client = m.source_client) AS total_issues_logged,
    (SELECT COUNT(*) FROM data_quality.dq_duplicate_tracker WHERE batch_id = m.batch_id AND source_client = m.source_client) AS duplicate_groups
FROM data_quality.dq_summary_metrics m
ORDER BY m.metric_date DESC, m.source_client;

-- ============================================================================
-- ISSUE TREND BY DATE
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_issue_trend_by_date AS
SELECT
    DATE_TRUNC('day', detected_at)::DATE AS issue_date,
    source_client,
    issue_category,
    issue_severity,
    COUNT(*) AS issue_count
FROM data_quality.dq_issue_log
GROUP BY DATE_TRUNC('day', detected_at)::DATE, source_client, issue_category, issue_severity
ORDER BY issue_date DESC, source_client, issue_count DESC;

-- ============================================================================
-- CUSTOMER DATA QUALITY
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_customer_data_quality AS
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.email_validation_status,
    c.loyalty_tier,
    c.source_client,
    COUNT(DISTINCT ft.transaction_sk) AS transaction_count,
    SUM(ft.payment_amount_abs) AS total_spend,
    SUM(CASE WHEN ft.has_dq_issues THEN 1 ELSE 0 END) AS transactions_with_issues,
    MAX(ft.order_date) AS last_order_date
FROM canonical.dim_customer c
LEFT JOIN canonical.fact_transaction ft ON c.customer_sk = ft.customer_sk
WHERE c.is_current = TRUE
  AND c.customer_sk != -1
GROUP BY c.customer_id, c.full_name, c.email, c.email_validation_status, 
         c.loyalty_tier, c.source_client
ORDER BY transaction_count DESC;

-- ============================================================================
-- RECONCILIATION VIEW
-- ============================================================================
CREATE OR REPLACE VIEW data_quality.v_reconciliation_summary AS
SELECT
    'RAW_XML' AS layer, 
    'raw.raw_xml_transactions' AS table_name,
    COUNT(*) AS record_count
FROM raw.raw_xml_transactions

UNION ALL

SELECT
    'RAW_JSON', 
    'raw.raw_json_transactions',
    COUNT(*)
FROM raw.raw_json_transactions

UNION ALL

SELECT
    'STAGING_CLIENT_A', 
    'staging.v_stg_client_a_transactions',
    COUNT(*)
FROM staging.v_stg_client_a_transactions

UNION ALL

SELECT
    'STAGING_CLIENT_C', 
    'staging.v_stg_client_c_transactions',
    COUNT(*)
FROM staging.v_stg_client_c_transactions

UNION ALL

SELECT
    'CANONICAL_TRANSACTIONS', 
    'canonical.fact_transaction',
    COUNT(*)
FROM canonical.fact_transaction

UNION ALL

SELECT
    'CANONICAL_ITEMS', 
    'canonical.fact_transaction_item',
    COUNT(*)
FROM canonical.fact_transaction_item

UNION ALL

SELECT
    'DQ_ISSUES', 
    'data_quality.dq_issue_log',
    COUNT(*)
FROM data_quality.dq_issue_log

UNION ALL

SELECT
    'DQ_DUPLICATES', 
    'data_quality.dq_duplicate_tracker',
    COUNT(*)
FROM data_quality.dq_duplicate_tracker

ORDER BY layer;

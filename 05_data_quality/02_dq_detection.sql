/*
================================================================================
DATA QUALITY: DETECTION PROCEDURES
================================================================================
Purpose: Detect and log data quality issues across all data layers
Author:  Data Platform Team
Version: 2.0
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA data_quality;

-- ============================================================================
-- PROCEDURE: Detect Duplicate Transactions
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_detect_duplicate_transactions(batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    duplicates_found NUMBER := 0;
BEGIN
    -- Detect duplicates in ClientA
    INSERT INTO data_quality.dq_duplicate_tracker (
        batch_id, source_client, duplicate_type, duplicate_key, duplicate_key_hash,
        occurrence_count, first_occurrence_file, all_occurrence_files, resolution_method
    )
    SELECT
        :batch_id,
        'ClientA',
        'TRANSACTION_ID',
        transaction_id,
        SHA2(transaction_id, 256),
        COUNT(*) AS occurrence_count,
        MIN(source_file_name),
        ARRAY_AGG(DISTINCT source_file_name),
        'KEEP_FIRST'
    FROM staging.v_stg_client_a_transactions
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING COUNT(*) > 1;
    
    duplicates_found := duplicates_found + SQLROWCOUNT;
    
    -- Detect duplicates in ClientC
    INSERT INTO data_quality.dq_duplicate_tracker (
        batch_id, source_client, duplicate_type, duplicate_key, duplicate_key_hash,
        occurrence_count, first_occurrence_file, all_occurrence_files, resolution_method
    )
    SELECT
        :batch_id,
        'ClientC',
        'TRANSACTION_ID',
        transaction_id,
        SHA2(transaction_id, 256),
        COUNT(*) AS occurrence_count,
        MIN(source_file_name),
        ARRAY_AGG(DISTINCT source_file_name),
        'KEEP_FIRST'
    FROM staging.v_stg_client_c_transactions
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING COUNT(*) > 1;
    
    duplicates_found := duplicates_found + SQLROWCOUNT;
    
    -- Detect duplicate Order IDs across transactions
    INSERT INTO data_quality.dq_duplicate_tracker (
        batch_id, source_client, duplicate_type, duplicate_key, duplicate_key_hash,
        occurrence_count, first_occurrence_file, all_occurrence_files, resolution_method
    )
    SELECT
        :batch_id,
        'ClientA',
        'ORDER_ID',
        order_id,
        SHA2(order_id, 256),
        COUNT(*) AS occurrence_count,
        MIN(source_file_name),
        ARRAY_AGG(DISTINCT source_file_name),
        'ALLOW_MULTIPLE'
    FROM staging.v_stg_client_a_transactions
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING COUNT(*) > 1;
    
    duplicates_found := duplicates_found + SQLROWCOUNT;
    
    -- Log to issue log
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_table, issue_type, issue_category, 
        issue_severity, issue_description, record_identifier
    )
    SELECT
        batch_id,
        source_client,
        'staging.v_stg_' || LOWER(source_client) || '_transactions',
        'DUPLICATE_' || duplicate_type,
        'UNIQUENESS',
        CASE WHEN duplicate_type = 'TRANSACTION_ID' THEN 'CRITICAL' ELSE 'WARNING' END,
        'Found ' || occurrence_count || ' occurrences of ' || duplicate_type || ' = ' || duplicate_key,
        duplicate_key
    FROM data_quality.dq_duplicate_tracker
    WHERE batch_id = :batch_id;
    
    RETURN 'Duplicate detection completed. Found ' || duplicates_found || ' duplicate groups.';
END;
$$;

-- ============================================================================
-- PROCEDURE: Log DQ Issues from ClientA Transactions
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_log_issues_client_a(batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    issues_logged NUMBER := 0;
BEGIN
    -- Missing Transaction IDs
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name, 
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'MISSING_TRANSACTION_ID', 'COMPLETENESS', 'CRITICAL',
        'Transaction ID is missing or empty',
        COALESCE(order_id, 'UNKNOWN_' || raw_record_id), 'transaction_id', transaction_id_raw
    FROM staging.v_stg_client_a_transactions
    WHERE is_missing_transaction_id = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Missing Customer IDs
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'MISSING_CUSTOMER_ID', 'COMPLETENESS', 'HIGH',
        'Customer ID is missing or empty',
        transaction_id, 'customer_id', customer_id_raw
    FROM staging.v_stg_client_a_transactions
    WHERE is_missing_customer_id = TRUE
      AND is_missing_transaction_id = FALSE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Missing Order IDs
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'MISSING_ORDER_ID', 'COMPLETENESS', 'HIGH',
        'Order ID is missing or empty',
        transaction_id, 'order_id', order_id_raw
    FROM staging.v_stg_client_a_transactions
    WHERE is_missing_order_id = TRUE
      AND is_missing_transaction_id = FALSE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Missing Order Dates
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'MISSING_ORDER_DATE', 'COMPLETENESS', 'MEDIUM',
        'Order date is missing or empty',
        transaction_id, 'order_date', order_date_raw
    FROM staging.v_stg_client_a_transactions
    WHERE is_missing_order_date = TRUE
      AND is_missing_transaction_id = FALSE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Invalid Emails
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw, field_value_expected
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.dim_customer',
        'INVALID_EMAIL_FORMAT', 'FORMAT', 'LOW',
        'Email format is invalid',
        transaction_id, 'email', email_raw, 'user@domain.com'
    FROM staging.v_stg_client_a_transactions
    WHERE email_validation_status = 'INVALID';
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Missing Payment Methods
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'MISSING_PAYMENT_METHOD', 'COMPLETENESS', 'MEDIUM',
        'Payment method is missing or empty',
        transaction_id, 'payment_method', payment_method_raw
    FROM staging.v_stg_client_a_transactions
    WHERE is_missing_payment_method = TRUE
      AND is_missing_transaction_id = FALSE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Negative Payment Amounts
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw, field_value_expected
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_a_transactions', 'canonical.fact_transaction',
        'NEGATIVE_PAYMENT_AMOUNT', 'VALIDITY', 'HIGH',
        'Payment amount is negative: ' || payment_amount_raw,
        transaction_id, 'payment_amount', payment_amount_raw, '>= 0'
    FROM staging.v_stg_client_a_transactions
    WHERE is_negative_payment_amount = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Negative Quantities in Items
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw, field_value_expected
    )
    SELECT
        :batch_id, 'ClientA', source_file_name,
        'staging.v_stg_client_a_transaction_items', 'canonical.fact_transaction_item',
        'NEGATIVE_QUANTITY', 'VALIDITY', 'HIGH',
        'Item quantity is negative: ' || quantity_raw,
        transaction_id || '-' || COALESCE(sku, 'LINE_' || line_number), 'quantity', quantity_raw, '>= 0'
    FROM staging.v_stg_client_a_transaction_items
    WHERE is_negative_quantity = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Negative Unit Prices
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw, field_value_expected
    )
    SELECT
        :batch_id, 'ClientA', source_file_name,
        'staging.v_stg_client_a_transaction_items', 'canonical.fact_transaction_item',
        'NEGATIVE_UNIT_PRICE', 'VALIDITY', 'HIGH',
        'Unit price is negative: ' || unit_price_raw,
        transaction_id || '-' || COALESCE(sku, 'LINE_' || line_number), 'unit_price', unit_price_raw, '>= 0'
    FROM staging.v_stg_client_a_transaction_items
    WHERE is_negative_unit_price = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Missing SKUs
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, 'ClientA', source_file_name,
        'staging.v_stg_client_a_transaction_items', 'canonical.fact_transaction_item',
        'MISSING_SKU', 'COMPLETENESS', 'MEDIUM',
        'Item SKU is missing or empty',
        transaction_id || '-LINE_' || line_number, 'sku', sku_raw
    FROM staging.v_stg_client_a_transaction_items
    WHERE is_missing_sku = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    RETURN 'ClientA DQ issues logged: ' || issues_logged || ' issues';
END;
$$;

-- ============================================================================
-- PROCEDURE: Log DQ Issues from ClientC Transactions
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_log_issues_client_c(batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    issues_logged NUMBER := 0;
BEGIN
    -- Similar structure to ClientA but for ClientC
    -- Missing Transaction IDs
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name, 
        'staging.v_stg_client_c_transactions', 'canonical.fact_transaction',
        'MISSING_TRANSACTION_ID', 'COMPLETENESS', 'CRITICAL',
        'Transaction ID is missing or empty',
        COALESCE(order_id, 'UNKNOWN_' || raw_record_id), 'transaction_id', transaction_id_raw
    FROM staging.v_stg_client_c_transactions
    WHERE is_missing_transaction_id = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Negative Payment Amounts
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, source_client, source_file_name,
        'staging.v_stg_client_c_transactions', 'canonical.fact_transaction',
        'NEGATIVE_PAYMENT_AMOUNT', 'VALIDITY', 'HIGH',
        'Payment amount is negative: ' || payment_amount_raw,
        transaction_id, 'payment_amount', payment_amount_raw
    FROM staging.v_stg_client_c_transactions
    WHERE is_negative_payment_amount = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    -- Negative Quantities in Items
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_file_name, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw
    )
    SELECT
        :batch_id, 'ClientC', source_file_name,
        'staging.v_stg_client_c_transaction_items', 'canonical.fact_transaction_item',
        'NEGATIVE_QUANTITY', 'VALIDITY', 'HIGH',
        'Item quantity is negative: ' || quantity_raw,
        transaction_id || '-' || COALESCE(sku, 'LINE_' || line_number), 'quantity', quantity_raw
    FROM staging.v_stg_client_c_transaction_items
    WHERE is_negative_quantity = TRUE;
    issues_logged := issues_logged + SQLROWCOUNT;
    
    RETURN 'ClientC DQ issues logged: ' || issues_logged || ' issues';
END;
$$;

-- ============================================================================
-- PROCEDURE: Calculate DQ Summary Metrics
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_calculate_dq_metrics(batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    -- Calculate metrics for all clients
    INSERT INTO data_quality.dq_summary_metrics (
        batch_id, source_client, 
        total_records_received, total_records_processed, total_records_loaded,
        total_records_rejected, total_records_warning,
        missing_field_count, invalid_format_count, invalid_value_count,
        negative_value_count, duplicate_count,
        rejection_rate, warning_rate, data_quality_score,
        calculation_duration_ms
    )
    WITH staging_counts AS (
        SELECT 'ClientA' AS source_client, COUNT(*) AS total_records
        FROM staging.v_stg_client_a_transactions
        UNION ALL
        SELECT 'ClientC', COUNT(*)
        FROM staging.v_stg_client_c_transactions
    ),
    canonical_counts AS (
        SELECT source_client, COUNT(*) AS loaded_records,
               SUM(CASE WHEN dq_status = 'INVALID' THEN 1 ELSE 0 END) AS rejected,
               SUM(CASE WHEN dq_status = 'WARNING' THEN 1 ELSE 0 END) AS warnings
        FROM canonical.fact_transaction
        GROUP BY source_client
    ),
    issue_counts AS (
        SELECT source_client,
               SUM(CASE WHEN issue_category = 'COMPLETENESS' THEN 1 ELSE 0 END) AS missing_count,
               SUM(CASE WHEN issue_category = 'FORMAT' THEN 1 ELSE 0 END) AS format_count,
               SUM(CASE WHEN issue_category = 'VALIDITY' THEN 1 ELSE 0 END) AS validity_count,
               SUM(CASE WHEN issue_type LIKE 'NEGATIVE%' THEN 1 ELSE 0 END) AS negative_count
        FROM data_quality.dq_issue_log
        WHERE batch_id = :batch_id
        GROUP BY source_client
    ),
    dup_counts AS (
        SELECT source_client, COUNT(*) AS dup_count
        FROM data_quality.dq_duplicate_tracker
        WHERE batch_id = :batch_id
        GROUP BY source_client
    )
    SELECT
        :batch_id,
        s.source_client,
        s.total_records,
        COALESCE(c.loaded_records, 0),
        COALESCE(c.loaded_records, 0),
        COALESCE(c.rejected, 0),
        COALESCE(c.warnings, 0),
        COALESCE(i.missing_count, 0),
        COALESCE(i.format_count, 0),
        COALESCE(i.validity_count, 0),
        COALESCE(i.negative_count, 0),
        COALESCE(d.dup_count, 0),
        ROUND(100.0 * COALESCE(c.rejected, 0) / NULLIF(s.total_records, 0), 2),
        ROUND(100.0 * COALESCE(c.warnings, 0) / NULLIF(s.total_records, 0), 2),
        ROUND(100.0 - (100.0 * (COALESCE(i.missing_count, 0) + COALESCE(i.validity_count, 0)) / NULLIF(s.total_records, 0)), 2),
        DATEDIFF('millisecond', :start_time, CURRENT_TIMESTAMP())
    FROM staging_counts s
    LEFT JOIN canonical_counts c ON s.source_client = c.source_client
    LEFT JOIN issue_counts i ON s.source_client = i.source_client
    LEFT JOIN dup_counts d ON s.source_client = d.source_client;
    
    RETURN 'DQ metrics calculated for batch ' || :batch_id;
END;
$$;

-- ============================================================================
-- PROCEDURE: Detect Payment vs Line Total Mismatches
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_detect_amount_mismatches(batch_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    mismatches_found NUMBER := 0;
BEGIN
    INSERT INTO data_quality.dq_issue_log (
        batch_id, source_client, source_table, target_table,
        issue_type, issue_category, issue_severity, issue_description,
        record_identifier, field_name, field_value_raw, field_value_expected
    )
    SELECT
        :batch_id,
        ft.source_client,
        'canonical.fact_transaction',
        'canonical.fact_transaction',
        'AMOUNT_MISMATCH',
        'CONSISTENCY',
        'WARNING',
        'Payment amount (' || ft.payment_amount_abs || ') does not match line total (' || ft.total_line_amount_abs || ')',
        ft.transaction_id,
        'payment_amount vs total_line_amount',
        ft.payment_amount_abs::VARCHAR,
        ft.total_line_amount_abs::VARCHAR
    FROM canonical.fact_transaction ft
    WHERE ft.total_line_amount_abs IS NOT NULL
      AND ft.payment_amount_abs IS NOT NULL
      AND ABS(ft.payment_amount_abs - ft.total_line_amount_abs) > 0.01
      AND ABS(ft.payment_amount_abs - ft.total_line_amount_abs) / NULLIF(ft.payment_amount_abs, 0) > 0.05;  -- 5% threshold
    
    mismatches_found := SQLROWCOUNT;
    
    RETURN 'Amount mismatch detection completed. Found ' || mismatches_found || ' mismatches.';
END;
$$;

-- ============================================================================
-- MASTER DQ DETECTION PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE data_quality.sp_run_all_dq_checks(batch_id VARCHAR)
RETURNS TABLE (check_name VARCHAR, result VARCHAR, completed_at TIMESTAMP_NTZ)
LANGUAGE SQL
AS
$$
DECLARE
    result_dup VARCHAR;
    result_a VARCHAR;
    result_c VARCHAR;
    result_mismatch VARCHAR;
    result_metrics VARCHAR;
BEGIN
    CALL data_quality.sp_detect_duplicate_transactions(:batch_id) INTO result_dup;
    CALL data_quality.sp_log_issues_client_a(:batch_id) INTO result_a;
    CALL data_quality.sp_log_issues_client_c(:batch_id) INTO result_c;
    CALL data_quality.sp_detect_amount_mismatches(:batch_id) INTO result_mismatch;
    CALL data_quality.sp_calculate_dq_metrics(:batch_id) INTO result_metrics;
    
    RETURN TABLE(
        SELECT 'Duplicate Detection' AS check_name, result_dup AS result, CURRENT_TIMESTAMP() AS completed_at
        UNION ALL SELECT 'ClientA Issues', result_a, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'ClientC Issues', result_c, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'Amount Mismatches', result_mismatch, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'Metrics Calculation', result_metrics, CURRENT_TIMESTAMP()
    );
END;
$$;

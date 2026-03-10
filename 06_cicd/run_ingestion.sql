/*
================================================================================
CI/CD: RUN INGESTION PIPELINE
================================================================================
Purpose: Execute complete ingestion pipeline
Author:  Data Platform Team
Version: 1.0

Prerequisites:
1. Deploy all objects using deploy_all.sql
2. Upload files to stages
================================================================================
*/

USE DATABASE financial_data_platform;
USE WAREHOUSE financial_etl_wh;

-- ============================================================================
-- STEP 1: GENERATE BATCH ID
-- ============================================================================
SET batch_id = (SELECT 'BATCH_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'));
SELECT 'Starting batch: ' || $batch_id AS status;

-- ============================================================================
-- STEP 2: LOAD RAW DATA
-- ============================================================================
SELECT 'Step 2: Loading raw data...' AS status;

-- Execute copy commands (source the script or run inline)
-- !source ../02_raw_ingestion/05_copy_into_raw.sql

-- ============================================================================
-- STEP 3: LOAD CANONICAL DATA
-- ============================================================================
SELECT 'Step 3: Loading canonical data...' AS status;

CALL canonical.sp_load_all();

-- ============================================================================
-- STEP 4: RUN DATA QUALITY CHECKS
-- ============================================================================
SELECT 'Step 4: Running data quality checks...' AS status;

CALL data_quality.sp_run_all_dq_checks($batch_id);

-- ============================================================================
-- STEP 5: GENERATE SUMMARY REPORT
-- ============================================================================
SELECT 'Step 5: Generating summary...' AS status;

SELECT '=== INGESTION SUMMARY ===' AS report;

SELECT * FROM data_quality.v_reconciliation_summary;

SELECT '=== DATA QUALITY SUMMARY ===' AS report;

SELECT * FROM data_quality.v_batch_quality_summary 
WHERE batch_id = $batch_id;

SELECT '=== FILE QUALITY ===' AS report;

SELECT * FROM data_quality.v_file_quality_summary;

SELECT 'Batch ' || $batch_id || ' completed at ' || CURRENT_TIMESTAMP() AS status;

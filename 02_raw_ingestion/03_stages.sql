/*
================================================================================
STAGES - INTERNAL STAGES FOR FILE INGESTION
================================================================================
Purpose: Create internal stages for each file type
Author:  Data Platform Team
Version: 2.0

Usage Examples:
  -- Upload files to stages:
  PUT file:///path/to/ClientA_Transactions_1.xml @raw.stg_xml_files AUTO_COMPRESS=FALSE;
  PUT file:///path/to/transactions.json @raw.stg_json_files AUTO_COMPRESS=FALSE;
  PUT file:///path/to/Customer.csv @raw.stg_csv_files AUTO_COMPRESS=FALSE;
  PUT file:///path/to/ClientA_Transactions_4.txt @raw.stg_txt_files AUTO_COMPRESS=FALSE;
  
  -- List staged files:
  LIST @raw.stg_xml_files;
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;

-- ============================================================================
-- XML FILES STAGE
-- For ClientA XML transaction files
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_xml_files
    FILE_FORMAT = raw.ff_xml
    COMMENT = 'Stage for XML transaction files (ClientA_Transactions_*.xml)';

-- ============================================================================
-- JSON FILES STAGE  
-- For ClientC JSON transaction files
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_json_files
    FILE_FORMAT = raw.ff_json
    COMMENT = 'Stage for JSON transaction files (transactions.json)';

-- ============================================================================
-- CSV FILES STAGE
-- For all CSV reference data files
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_csv_files
    FILE_FORMAT = raw.ff_csv_standard
    COMMENT = 'Stage for CSV reference files (Customer, Orders, Products, Payments)';

-- ============================================================================
-- TXT FILES STAGE
-- For TXT files containing XML content
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_txt_files
    FILE_FORMAT = raw.ff_txt_raw
    COMMENT = 'Stage for TXT files with XML content (ClientA_Transactions_4.txt)';

-- ============================================================================
-- ARCHIVE STAGE
-- For processed files archive
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_archive
    COMMENT = 'Archive stage for processed source files';

-- ============================================================================
-- ERROR STAGE
-- For files that failed processing
-- ============================================================================
CREATE OR REPLACE STAGE raw.stg_errors
    COMMENT = 'Stage for files that encountered processing errors';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW STAGES IN SCHEMA raw;

SELECT 'Stages created successfully' AS status;

/*
================================================================================
STAGES - INTERNAL STAGES FOR FILE INGESTION
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;

-- XML FILES STAGE
CREATE OR REPLACE STAGE raw.stg_xml_files
    FILE_FORMAT = raw.ff_xml
    COMMENT = 'Stage for XML transaction files (ClientA_Transactions_*.xml)';

-- JSON FILES STAGE
CREATE OR REPLACE STAGE raw.stg_json_files
    FILE_FORMAT = raw.ff_json
    COMMENT = 'Stage for JSON transaction files (transactions.json)';

-- CSV FILES STAGE
CREATE OR REPLACE STAGE raw.stg_csv_files
    FILE_FORMAT = raw.ff_csv_standard
    COMMENT = 'Stage for CSV reference files (Customer, Orders, Products, Payments)';

-- TXT FILES STAGE
CREATE OR REPLACE STAGE raw.stg_txt_files
    FILE_FORMAT = raw.ff_txt_raw
    COMMENT = 'Stage for TXT files with XML content (ClientA_Transactions_4.txt)';

-- ARCHIVE STAGE
CREATE OR REPLACE STAGE raw.stg_archive
    COMMENT = 'Archive stage for processed source files';

-- ERROR STAGE
CREATE OR REPLACE STAGE raw.stg_errors
    COMMENT = 'Stage for files that encountered processing errors';

-- Verification
SHOW STAGES IN SCHEMA raw;

SELECT 'Stages created successfully' AS status;

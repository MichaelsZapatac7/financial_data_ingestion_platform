/*
================================================================================
CI/CD: MASTER DEPLOYMENT SCRIPT - REVISED
================================================================================
Purpose: Execute all scripts in correct order for full deployment
Author:  Data Platform Team
Version: 2.1

Note: This script should be run from the repository root directory.
For GitHub Actions, use explicit script execution instead of !source.
================================================================================
*/

-- ============================================================================
-- DEPLOYMENT APPROACH: Inline execution or external orchestration
-- ============================================================================

-- Option 1: For SnowSQL CLI with working directory set
-- snowsql -f 06_cicd/deploy_all.sql

-- Option 2: For GitHub Actions - execute scripts individually
-- See .github/workflows/ci-cd.yml for orchestrated deployment

-- ============================================================================
-- PRE-DEPLOYMENT VALIDATION
-- ============================================================================
SELECT CURRENT_TIMESTAMP() AS deployment_started;
SELECT CURRENT_USER() AS deploying_user;
SELECT CURRENT_ROLE() AS current_role;

-- ============================================================================
-- DEPLOYMENT SEQUENCE (execute in order)
-- ============================================================================
/*
Execute these scripts in sequence:

1. 02_raw_ingestion/01_setup_database.sql
2. 02_raw_ingestion/02_file_formats.sql
3. 02_raw_ingestion/03_stages.sql
4. 02_raw_ingestion/04_raw_tables.sql
5. 03_staging/01_stg_client_a_transactions.sql
6. 03_staging/02_stg_client_c_transactions.sql
7. 03_staging/03_stg_csv_reference.sql
8. 04_canonical/01_canonical_dimensions.sql
9. 04_canonical/02_canonical_facts.sql
10. 04_canonical/03_load_canonical.sql
11. 05_data_quality/01_dq_tables.sql
12. 05_data_quality/02_dq_detection.sql
13. 05_data_quality/03_dq_reports.sql
*/

-- ============================================================================
-- POST-DEPLOYMENT VALIDATION
-- ============================================================================
SELECT 'DEPLOYMENT VALIDATION' AS check_type;

-- Verify all schemas exist
SELECT SCHEMA_NAME 
FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE CATALOG_NAME = 'FINANCIAL_DATA_PLATFORM'
ORDER BY SCHEMA_NAME;

-- Verify key objects
SELECT 'TABLES' AS object_type, COUNT(*) AS count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'FINANCIAL_DATA_PLATFORM'
UNION ALL
SELECT 'VIEWS', COUNT(*)
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_CATALOG = 'FINANCIAL_DATA_PLATFORM'
UNION ALL
SELECT 'PROCEDURES', COUNT(*)
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_CATALOG = 'FINANCIAL_DATA_PLATFORM';

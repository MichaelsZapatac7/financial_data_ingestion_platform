/*
================================================================================
DATABASE SETUP - COMPLETE CONFIGURATION
================================================================================
*/

-- Warehouse Configuration
CREATE WAREHOUSE IF NOT EXISTS financial_etl_wh
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 1
SCALING_POLICY = 'STANDARD'
COMMENT = 'ETL warehouse for financial data platform';

USE WAREHOUSE financial_etl_wh;

-- Database
CREATE DATABASE IF NOT EXISTS financial_data_platform
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Financial transaction data platform - multi-client ingestion';

USE DATABASE financial_data_platform;

-- Schemas (Layered Architecture)
CREATE SCHEMA IF NOT EXISTS raw
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw layer - preserves source data exactly as received';

CREATE SCHEMA IF NOT EXISTS staging
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'Staging layer - parsed and normalized data via views';

CREATE SCHEMA IF NOT EXISTS canonical
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Canonical layer - unified dimensional model for analytics';

CREATE SCHEMA IF NOT EXISTS data_quality
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Data quality layer - anomaly detection and rejection tracking';

-- Sequences for Surrogate Keys
CREATE OR REPLACE SEQUENCE raw.seq_raw_record_id START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE canonical.seq_transaction_sk START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE canonical.seq_customer_sk START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE canonical.seq_product_sk START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE canonical.seq_order_sk START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE canonical.seq_payment_sk START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE data_quality.seq_dq_issue_id START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE data_quality.seq_rejection_id START = 1 INCREMENT = 1;

-- Tags for Governance
CREATE TAG IF NOT EXISTS financial_data_platform.raw.pii_data
ALLOWED_VALUES 'TRUE', 'FALSE'
COMMENT = 'Indicates if column contains PII';

CREATE TAG IF NOT EXISTS financial_data_platform.raw.data_classification
ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
COMMENT = 'Data classification level';

-- Verification
SHOW SCHEMAS IN DATABASE financial_data_platform;
SHOW SEQUENCES IN SCHEMA raw;
SHOW SEQUENCES IN SCHEMA canonical;
SHOW SEQUENCES IN SCHEMA data_quality;

SELECT
    'Database setup completed successfully' AS status,
    CURRENT_TIMESTAMP() AS completed_at;

/*
================================================================================
RAW TABLES - SOURCE FIDELITY PRESERVATION
================================================================================
Purpose: Create raw tables that preserve source data exactly as received
Author:  Data Platform Team
Version: 2.0

Design Principles:
- VARIANT columns for semi-structured data (XML, JSON)
- All string columns for CSV (no type coercion at raw layer)
- Full audit trail (source file, batch, timestamp, hash)
- No data loss - everything preserved
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;

-- ============================================================================
-- RAW XML TRANSACTIONS (ClientA)
-- Stores complete XML documents from ClientA
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_xml_transactions (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Content (VARIANT preserves full XML structure)
    raw_xml_content         VARIANT NOT NULL,
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    source_file_path        VARCHAR(1000),
    source_client           VARCHAR(50) DEFAULT 'ClientA',
    source_format           VARCHAR(20) DEFAULT 'XML',
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    file_size_bytes         NUMBER,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_user          VARCHAR(100) DEFAULT CURRENT_USER(),
    processing_notes        VARCHAR(2000),
    
    -- Constraints
    CONSTRAINT pk_raw_xml_transactions PRIMARY KEY (raw_record_id)
)
CLUSTER BY (source_file_name, ingestion_timestamp)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE
COMMENT = 'Raw XML transactions from ClientA - complete document preservation';

-- ============================================================================
-- RAW TXT-XML TRANSACTIONS (ClientA - TXT files with XML content)
-- Special handling for .txt files containing XML
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_txt_xml_transactions (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Content
    raw_text_content        VARCHAR(16777216),    -- Full text as received
    raw_xml_content         VARIANT,               -- Parsed XML (after processing)
    
    -- Parse Status
    xml_parse_success       BOOLEAN DEFAULT FALSE,
    xml_parse_error         VARCHAR(2000),
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    source_client           VARCHAR(50) DEFAULT 'ClientA',
    source_format           VARCHAR(20) DEFAULT 'TXT_XML',
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ,
    processing_notes        VARCHAR(2000),
    
    -- Constraints
    CONSTRAINT pk_raw_txt_xml_transactions PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw TXT files containing XML content - requires post-processing';

-- ============================================================================
-- RAW JSON TRANSACTIONS (ClientC)
-- Stores complete JSON documents from ClientC
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_json_transactions (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Content (VARIANT preserves full JSON structure)
    raw_json_content        VARIANT NOT NULL,
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    source_client           VARCHAR(50) DEFAULT 'ClientC',
    source_format           VARCHAR(20) DEFAULT 'JSON',
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_notes        VARCHAR(2000),
    
    -- Constraints
    CONSTRAINT pk_raw_json_transactions PRIMARY KEY (raw_record_id)
)
CLUSTER BY (ingestion_timestamp)
COMMENT = 'Raw JSON transactions from ClientC';

-- ============================================================================
-- RAW CSV CUSTOMERS
-- Reference data - all fields as VARCHAR to preserve source exactly
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_csv_customers (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Fields (all VARCHAR - no type coercion)
    customer_id_raw         VARCHAR(100),
    first_name_raw          VARCHAR(200),
    last_name_raw           VARCHAR(200),
    email_raw               VARCHAR(500),
    loyalty_tier_raw        VARCHAR(50),
    signup_source_raw       VARCHAR(100),
    is_active_raw           VARCHAR(10),
    
    -- Extra columns for schema flexibility
    extra_col_1             VARCHAR(500),
    extra_col_2             VARCHAR(500),
    extra_col_3             VARCHAR(500),
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    file_row_number         NUMBER,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_raw_csv_customers PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw customer reference data from CSV files';

-- ============================================================================
-- RAW CSV ORDERS
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_csv_orders (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Fields
    order_id_raw            VARCHAR(100),
    customer_id_raw         VARCHAR(100),
    order_date_raw          VARCHAR(50),
    status_raw              VARCHAR(50),
    total_amount_raw        VARCHAR(50),
    currency_raw            VARCHAR(10),
    
    -- Extra columns
    extra_col_1             VARCHAR(500),
    extra_col_2             VARCHAR(500),
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    file_row_number         NUMBER,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_raw_csv_orders PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw order reference data from CSV files';

-- ============================================================================
-- RAW CSV PRODUCTS
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_csv_products (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Fields
    product_id_raw          VARCHAR(100),
    sku_raw                 VARCHAR(100),
    product_name_raw        VARCHAR(500),
    description_raw         VARCHAR(2000),
    category_raw            VARCHAR(200),
    price_raw               VARCHAR(50),
    currency_raw            VARCHAR(10),
    
    -- Extra columns
    extra_col_1             VARCHAR(500),
    extra_col_2             VARCHAR(500),
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    file_row_number         NUMBER,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_raw_csv_products PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw product reference data from CSV files';

-- ============================================================================
-- RAW CSV PAYMENTS
-- ============================================================================
CREATE OR REPLACE TABLE raw.raw_csv_payments (
    -- Primary Key
    raw_record_id           NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    
    -- Raw Fields
    payment_id_raw          VARCHAR(100),
    order_id_raw            VARCHAR(100),
    transaction_id_raw      VARCHAR(100),
    payment_method_raw      VARCHAR(50),
    amount_raw              VARCHAR(50),
    currency_raw            VARCHAR(10),
    payment_date_raw        VARCHAR(50),
    status_raw              VARCHAR(50),
    
    -- Extra columns
    extra_col_1             VARCHAR(500),
    extra_col_2             VARCHAR(500),
    
    -- Source Tracking
    source_file_name        VARCHAR(500) NOT NULL,
    
    -- Batch & Processing
    batch_id                VARCHAR(100) NOT NULL,
    file_row_number         NUMBER,
    record_hash             VARCHAR(64),
    
    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_raw_csv_payments PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw payment reference data from CSV files';

-- ============================================================================
-- INGESTION AUDIT LOG
-- Tracks all ingestion operations
-- ============================================================================
CREATE OR REPLACE TABLE raw.ingestion_audit_log (
    -- Primary Key
    audit_id                NUMBER AUTOINCREMENT,
    
    -- Batch Identification
    batch_id                VARCHAR(100) NOT NULL,
    
    -- Source Information
    source_file_name        VARCHAR(500),
    source_file_pattern     VARCHAR(200),
    target_table            VARCHAR(200) NOT NULL,
    target_schema           VARCHAR(100) DEFAULT 'RAW',
    
    -- Timing
    ingestion_start_time    TIMESTAMP_NTZ,
    ingestion_end_time      TIMESTAMP_NTZ,
    duration_seconds        NUMBER,
    
    -- Metrics
    rows_loaded             NUMBER DEFAULT 0,
    rows_rejected           NUMBER DEFAULT 0,
    rows_parsed             NUMBER DEFAULT 0,
    
    -- Status
    status                  VARCHAR(20) NOT NULL,
    error_message           VARCHAR(4000),
    error_details           VARIANT,
    
    -- Audit
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by              VARCHAR(100) DEFAULT CURRENT_USER(),
    
    -- Constraints
    CONSTRAINT pk_ingestion_audit_log PRIMARY KEY (audit_id)
)
CLUSTER BY (batch_id, created_at)
COMMENT = 'Audit log tracking all file ingestion operations';

-- ============================================================================
-- COPY HISTORY EXTENSION
-- Captures detailed copy operation results
-- ============================================================================
CREATE OR REPLACE TABLE raw.copy_history_detail (
    copy_id                 NUMBER AUTOINCREMENT,
    batch_id                VARCHAR(100),
    file_name               VARCHAR(500),
    stage_location          VARCHAR(500),
    target_table            VARCHAR(200),
    row_count               NUMBER,
    row_parsed              NUMBER,
    file_size               NUMBER,
    first_error_message     VARCHAR(4000),
    first_error_line        NUMBER,
    first_error_column      NUMBER,
    first_error_column_name VARCHAR(200),
    status                  VARCHAR(20),
    copy_timestamp          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_copy_history_detail PRIMARY KEY (copy_id)
)
COMMENT = 'Detailed copy operation history';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'RAW TABLES CREATED' AS status;

SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    CREATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW'
ORDER BY TABLE_NAME;

/*
================================================================================
RAW TABLES - SOURCE FIDELITY PRESERVATION
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;

-- RAW XML TRANSACTIONS (ClientA)
CREATE OR REPLACE TABLE raw.raw_xml_transactions (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    raw_xml_content VARIANT NOT NULL,
    source_file_name VARCHAR(500) NOT NULL,
    source_file_path VARCHAR(1000),
    source_client VARCHAR(50) DEFAULT 'ClientA',
    source_format VARCHAR(20) DEFAULT 'XML',
    batch_id VARCHAR(100) NOT NULL,
    file_size_bytes NUMBER,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ingestion_user VARCHAR(100) DEFAULT CURRENT_USER(),
    processing_notes VARCHAR(2000),
    CONSTRAINT pk_raw_xml_transactions PRIMARY KEY (raw_record_id)
)
CLUSTER BY (source_file_name, ingestion_timestamp)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE
COMMENT = 'Raw XML transactions from ClientA';

-- RAW TXT-XML TRANSACTIONS
CREATE OR REPLACE TABLE raw.raw_txt_xml_transactions (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    raw_text_content VARCHAR(16777216),
    raw_xml_content VARIANT,
    xml_parse_success BOOLEAN DEFAULT FALSE,
    xml_parse_error VARCHAR(2000),
    source_file_name VARCHAR(500) NOT NULL,
    source_client VARCHAR(50) DEFAULT 'ClientA',
    source_format VARCHAR(20) DEFAULT 'TXT_XML',
    batch_id VARCHAR(100) NOT NULL,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ,
    processing_notes VARCHAR(2000),
    CONSTRAINT pk_raw_txt_xml_transactions PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw TXT files containing XML content';

-- RAW JSON TRANSACTIONS (ClientC)
CREATE OR REPLACE TABLE raw.raw_json_transactions (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    raw_json_content VARIANT NOT NULL,
    source_file_name VARCHAR(500) NOT NULL,
    source_client VARCHAR(50) DEFAULT 'ClientC',
    source_format VARCHAR(20) DEFAULT 'JSON',
    batch_id VARCHAR(100) NOT NULL,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processing_notes VARCHAR(2000),
    CONSTRAINT pk_raw_json_transactions PRIMARY KEY (raw_record_id)
)
CLUSTER BY (ingestion_timestamp)
COMMENT = 'Raw JSON transactions from ClientC';

-- RAW CSV CUSTOMERS
CREATE OR REPLACE TABLE raw.raw_csv_customers (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    customer_id_raw VARCHAR(100),
    first_name_raw VARCHAR(200),
    last_name_raw VARCHAR(200),
    email_raw VARCHAR(500),
    loyalty_tier_raw VARCHAR(50),
    signup_source_raw VARCHAR(100),
    is_active_raw VARCHAR(10),
    extra_col_1 VARCHAR(500),
    extra_col_2 VARCHAR(500),
    extra_col_3 VARCHAR(500),
    source_file_name VARCHAR(500) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    file_row_number NUMBER,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_csv_customers PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw customer reference data from CSV files';

-- RAW CSV ORDERS
CREATE OR REPLACE TABLE raw.raw_csv_orders (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    order_id_raw VARCHAR(100),
    customer_id_raw VARCHAR(100),
    order_date_raw VARCHAR(50),
    status_raw VARCHAR(50),
    total_amount_raw VARCHAR(50),
    currency_raw VARCHAR(10),
    extra_col_1 VARCHAR(500),
    extra_col_2 VARCHAR(500),
    source_file_name VARCHAR(500) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    file_row_number NUMBER,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_csv_orders PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw order reference data from CSV files';

-- RAW CSV PRODUCTS
CREATE OR REPLACE TABLE raw.raw_csv_products (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    product_id_raw VARCHAR(100),
    sku_raw VARCHAR(100),
    product_name_raw VARCHAR(500),
    description_raw VARCHAR(2000),
    category_raw VARCHAR(200),
    price_raw VARCHAR(50),
    currency_raw VARCHAR(10),
    extra_col_1 VARCHAR(500),
    extra_col_2 VARCHAR(500),
    source_file_name VARCHAR(500) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    file_row_number NUMBER,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_csv_products PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw product reference data from CSV files';

-- RAW CSV PAYMENTS
CREATE OR REPLACE TABLE raw.raw_csv_payments (
    raw_record_id NUMBER DEFAULT raw.seq_raw_record_id.NEXTVAL,
    payment_id_raw VARCHAR(100),
    order_id_raw VARCHAR(100),
    transaction_id_raw VARCHAR(100),
    payment_method_raw VARCHAR(50),
    amount_raw VARCHAR(50),
    currency_raw VARCHAR(10),
    payment_date_raw VARCHAR(50),
    status_raw VARCHAR(50),
    extra_col_1 VARCHAR(500),
    extra_col_2 VARCHAR(500),
    source_file_name VARCHAR(500) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    file_row_number NUMBER,
    record_hash VARCHAR(64),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_csv_payments PRIMARY KEY (raw_record_id)
)
COMMENT = 'Raw payment reference data from CSV files';

-- INGESTION AUDIT LOG
CREATE OR REPLACE TABLE raw.ingestion_audit_log (
    audit_id NUMBER AUTOINCREMENT,
    batch_id VARCHAR(100) NOT NULL,
    source_file_name VARCHAR(500),
    source_file_pattern VARCHAR(200),
    target_table VARCHAR(200) NOT NULL,
    target_schema VARCHAR(100) DEFAULT 'RAW',
    ingestion_start_time TIMESTAMP_NTZ,
    ingestion_end_time TIMESTAMP_NTZ,
    duration_seconds NUMBER,
    rows_loaded NUMBER DEFAULT 0,
    rows_rejected NUMBER DEFAULT 0,
    rows_parsed NUMBER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(4000),
    error_details VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    CONSTRAINT pk_ingestion_audit_log PRIMARY KEY (audit_id)
)
CLUSTER BY (batch_id, created_at)
COMMENT = 'Audit log tracking all file ingestion operations';

-- Verification
SELECT 'RAW TABLES CREATED' AS status;

SELECT
    table_name,
    comment
FROM information_schema.tables
WHERE table_schema = 'RAW'
ORDER BY table_name;

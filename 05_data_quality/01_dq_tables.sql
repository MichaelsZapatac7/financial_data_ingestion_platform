/*
================================================================================
DATA QUALITY: TRACKING TABLES - COMPLETE
================================================================================
Purpose: Create tables for data quality issue tracking and rejected records
Author:  Data Platform Team
Version: 2.0
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA data_quality;

-- ============================================================================
-- DQ_ISSUE_LOG - Detailed issue tracking
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_issue_log (
    dq_issue_id              NUMBER DEFAULT data_quality.seq_dq_issue_id.NEXTVAL,
    
    -- Batch Info
    batch_id                 VARCHAR(100),
    check_execution_id       VARCHAR(100),
    
    -- Source Info
    source_client            VARCHAR(50),
    source_file_name         VARCHAR(500),
    source_table             VARCHAR(200),
    target_table             VARCHAR(200),
    
    -- Issue Classification
    issue_type               VARCHAR(100) NOT NULL,
    issue_category           VARCHAR(50) NOT NULL,
    issue_severity           VARCHAR(20) NOT NULL,
    issue_description        VARCHAR(2000),
    
    -- Affected Record
    record_identifier        VARCHAR(500),
    field_name               VARCHAR(200),
    field_value_raw          VARCHAR(2000),
    field_value_expected     VARCHAR(2000),
    
    -- Context
    raw_record               VARIANT,
    additional_context       VARIANT,
    
    -- Resolution
    resolution_action        VARCHAR(100),
    resolution_notes         VARCHAR(2000),
    is_resolved              BOOLEAN DEFAULT FALSE,
    resolved_at              TIMESTAMP_NTZ,
    resolved_by              VARCHAR(100),
    
    -- Audit
    detected_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    detected_by              VARCHAR(100) DEFAULT CURRENT_USER(),
    
    CONSTRAINT pk_dq_issue_log PRIMARY KEY (dq_issue_id)
)
CLUSTER BY (batch_id, issue_category, detected_at)
COMMENT = 'Detailed log of all data quality issues detected';

-- ============================================================================
-- DQ_REJECTED_RECORDS - Records that failed critical validation
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_rejected_records (
    rejection_id             NUMBER DEFAULT data_quality.seq_rejection_id.NEXTVAL,
    
    -- Batch Info
    batch_id                 VARCHAR(100),
    
    -- Source Info
    source_client            VARCHAR(50),
    source_file_name         VARCHAR(500),
    source_format            VARCHAR(20),
    
    -- Record Identification
    record_identifier        VARCHAR(500),
    transaction_id           VARCHAR(100),
    order_id                 VARCHAR(100),
    customer_id              VARCHAR(100),
    
    -- Rejection Details
    rejection_reason         VARCHAR(2000) NOT NULL,
    rejection_category       VARCHAR(100),
    rejection_severity       VARCHAR(20) DEFAULT 'CRITICAL',
    rejection_codes          VARIANT,
    
    -- Full Record
    raw_record               VARIANT,
    parsed_record            VARIANT,
    
    -- Processing
    can_be_reprocessed       BOOLEAN DEFAULT TRUE,
    reprocess_instructions   VARCHAR(2000),
    reprocessed              BOOLEAN DEFAULT FALSE,
    reprocessed_at           TIMESTAMP_NTZ,
    reprocessed_by           VARCHAR(100),
    
    -- Audit
    rejected_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rejected_by              VARCHAR(100) DEFAULT CURRENT_USER(),
    
    CONSTRAINT pk_dq_rejected_records PRIMARY KEY (rejection_id)
)
CLUSTER BY (batch_id, source_client, rejected_at)
COMMENT = 'Records that failed critical validation and were rejected';

-- ============================================================================
-- DQ_DUPLICATE_TRACKER - Track duplicate records
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_duplicate_tracker (
    duplicate_id             NUMBER AUTOINCREMENT,
    
    -- Batch Info
    batch_id                 VARCHAR(100),
    
    -- Source Info
    source_client            VARCHAR(50),
    
    -- Duplicate Details
    duplicate_type           VARCHAR(50) NOT NULL,
    duplicate_key            VARCHAR(500) NOT NULL,
    duplicate_key_hash       VARCHAR(64),
    occurrence_count         NUMBER NOT NULL,
    
    -- First Occurrence
    first_occurrence_file    VARCHAR(500),
    first_occurrence_record_id NUMBER,
    first_occurrence_timestamp TIMESTAMP_NTZ,
    
    -- All Occurrences
    all_occurrence_files     VARIANT,
    all_occurrence_record_ids VARIANT,
    
    -- Resolution
    resolution_method        VARCHAR(100),
    retained_record_id       NUMBER,
    discarded_record_ids     VARIANT,
    
    -- Audit
    detected_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dq_duplicate_tracker PRIMARY KEY (duplicate_id)
)
CLUSTER BY (batch_id, duplicate_type, detected_at)
COMMENT = 'Tracks duplicate records detected during processing';

-- ============================================================================
-- DQ_VALIDATION_RULES - Define validation rules
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_validation_rules (
    rule_id                  NUMBER AUTOINCREMENT,
    
    -- Rule Definition
    rule_code                VARCHAR(50) NOT NULL,
    rule_name                VARCHAR(200) NOT NULL,
    rule_description         VARCHAR(2000),
    rule_category            VARCHAR(50),
    
    -- Target
    target_table             VARCHAR(200),
    target_column            VARCHAR(200),
    
    -- Rule Logic
    rule_type                VARCHAR(50) NOT NULL,
    rule_expression          VARCHAR(4000),
    expected_result          VARCHAR(100),
    
    -- Severity & Action
    severity                 VARCHAR(20) DEFAULT 'WARNING',
    action_on_failure        VARCHAR(50) DEFAULT 'LOG',
    
    -- Status
    is_active                BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by               VARCHAR(100) DEFAULT CURRENT_USER(),
    updated_at               TIMESTAMP_NTZ,
    updated_by               VARCHAR(100),
    
    CONSTRAINT pk_dq_validation_rules PRIMARY KEY (rule_id),
    CONSTRAINT uk_dq_validation_rules UNIQUE (rule_code)
)
COMMENT = 'Data quality validation rules configuration';

-- Seed validation rules
INSERT INTO data_quality.dq_validation_rules (rule_code, rule_name, rule_description, rule_category, target_table, target_column, rule_type, severity, action_on_failure)
VALUES
    ('R001', 'Transaction ID Required', 'Transaction ID must not be null or empty', 'COMPLETENESS', 'fact_transaction', 'transaction_id', 'NOT_NULL', 'CRITICAL', 'REJECT'),
    ('R002', 'Order ID Required', 'Order ID must not be null or empty', 'COMPLETENESS', 'fact_transaction', 'order_id', 'NOT_NULL', 'HIGH', 'LOG'),
    ('R003', 'Customer ID Required', 'Customer ID must not be null or empty', 'COMPLETENESS', 'fact_transaction', 'customer_id', 'NOT_NULL', 'HIGH', 'LOG'),
    ('R004', 'Valid Email Format', 'Email must match valid email pattern', 'FORMAT', 'dim_customer', 'email', 'REGEX', 'MEDIUM', 'LOG'),
    ('R005', 'Positive Payment Amount', 'Payment amount should be positive', 'VALIDITY', 'fact_transaction', 'payment_amount', 'POSITIVE', 'HIGH', 'LOG'),
    ('R006', 'Positive Quantity', 'Item quantity should be positive', 'VALIDITY', 'fact_transaction_item', 'quantity', 'POSITIVE', 'HIGH', 'LOG'),
    ('R007', 'Positive Unit Price', 'Unit price should be positive', 'VALIDITY', 'fact_transaction_item', 'unit_price', 'POSITIVE', 'HIGH', 'LOG'),
    ('R008', 'SKU Required', 'SKU must not be null or empty', 'COMPLETENESS', 'fact_transaction_item', 'sku', 'NOT_NULL', 'MEDIUM', 'LOG'),
    ('R009', 'No Duplicate Transactions', 'Transaction ID should be unique', 'UNIQUENESS', 'fact_transaction', 'transaction_id', 'UNIQUE', 'CRITICAL', 'REJECT'),
    ('R010', 'Valid Order Date', 'Order date should parse correctly', 'FORMAT', 'fact_transaction', 'order_date', 'NOT_NULL', 'MEDIUM', 'LOG');

-- ============================================================================
-- DQ_SUMMARY_METRICS - Summary metrics per batch
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_summary_metrics (
    metric_id                NUMBER AUTOINCREMENT,
    
    -- Batch Info
    batch_id                 VARCHAR(100) NOT NULL,
    metric_date              DATE DEFAULT CURRENT_DATE(),
    source_client            VARCHAR(50),
    
    -- Volume Metrics
    total_records_received   NUMBER DEFAULT 0,
    total_records_processed  NUMBER DEFAULT 0,
    total_records_loaded     NUMBER DEFAULT 0,
    total_records_rejected   NUMBER DEFAULT 0,
    total_records_warning    NUMBER DEFAULT 0,
    
    -- Issue Counts by Category
    missing_field_count      NUMBER DEFAULT 0,
    invalid_format_count     NUMBER DEFAULT 0,
    invalid_value_count      NUMBER DEFAULT 0,
    negative_value_count     NUMBER DEFAULT 0,
    duplicate_count          NUMBER DEFAULT 0,
    referential_integrity_count NUMBER DEFAULT 0,
    
    -- Calculated Metrics
    rejection_rate           DECIMAL(5,2),
    warning_rate             DECIMAL(5,2),
    data_quality_score       DECIMAL(5,2),
    completeness_score       DECIMAL(5,2),
    validity_score           DECIMAL(5,2),
    
    -- Audit
    calculated_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    calculation_duration_ms  NUMBER,
    
    CONSTRAINT pk_dq_summary_metrics PRIMARY KEY (metric_id)
)
CLUSTER BY (batch_id, metric_date)
COMMENT = 'Summary metrics for data quality monitoring per batch';

-- ============================================================================
-- DQ_ANOMALY_PATTERNS - Track recurring anomaly patterns
-- ============================================================================
CREATE OR REPLACE TABLE data_quality.dq_anomaly_patterns (
    pattern_id               NUMBER AUTOINCREMENT,
    
    -- Pattern Definition
    pattern_name             VARCHAR(200) NOT NULL,
    pattern_description      VARCHAR(2000),
    pattern_category         VARCHAR(50),
    
    -- Detection
    detection_query          VARCHAR(4000),
    affected_clients         VARIANT,
    affected_fields          VARIANT,
    
    -- Frequency
    first_detected_at        TIMESTAMP_NTZ,
    last_detected_at         TIMESTAMP_NTZ,
    occurrence_count         NUMBER DEFAULT 1,
    
    -- Impact
    estimated_record_impact  NUMBER,
    business_impact          VARCHAR(50),
    
    -- Resolution
    resolution_status        VARCHAR(50) DEFAULT 'OPEN',
    resolution_notes         VARCHAR(2000),
    
    -- Audit
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dq_anomaly_patterns PRIMARY KEY (pattern_id)
)
COMMENT = 'Tracks recurring data quality anomaly patterns';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'DATA QUALITY TABLES CREATED' AS status;

SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DATA_QUALITY'
ORDER BY TABLE_NAME;

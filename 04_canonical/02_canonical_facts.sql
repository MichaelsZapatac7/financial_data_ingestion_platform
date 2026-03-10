/*
================================================================================
CANONICAL: FACT TABLES
================================================================================
Purpose: Create unified fact tables for analytics
Author:  Data Platform Team
Version: 2.0

Facts:
- fact_transaction: Transaction header (grain = one transaction)
- fact_transaction_item: Line items (grain = one item per transaction)
- fact_order: Orders from reference data
- fact_payment: Payments from reference data
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA canonical;

-- ============================================================================
-- FACT_TRANSACTION (Header Level)
-- Grain: One row per unique transaction
-- ============================================================================
CREATE OR REPLACE TABLE canonical.fact_transaction (
    -- Surrogate Key
    transaction_sk           NUMBER DEFAULT canonical.seq_transaction_sk.NEXTVAL,
    
    -- Business Keys
    transaction_id           VARCHAR(100) NOT NULL,
    order_id                 VARCHAR(100),
    
    -- Foreign Keys to Dimensions
    customer_sk              NUMBER DEFAULT -1,
    order_date_sk            NUMBER DEFAULT -1,
    payment_method_sk        NUMBER,
    
    -- Degenerate Dimensions
    customer_id              VARCHAR(100),
    order_date               DATE,
    
    -- Measures
    payment_amount           DECIMAL(18,4),
    payment_amount_abs       DECIMAL(18,4),
    payment_currency         VARCHAR(10) DEFAULT 'USD',
    item_count               NUMBER,
    total_line_amount        DECIMAL(18,4),
    total_line_amount_abs    DECIMAL(18,4),
    
    -- Variance (payment vs line total)
    amount_variance          DECIMAL(18,4),
    has_amount_variance      BOOLEAN DEFAULT FALSE,
    
    -- Source Tracking
    source_client            VARCHAR(50) NOT NULL,
    source_file_name         VARCHAR(500),
    source_format            VARCHAR(20),
    
    -- Data Quality
    dq_status                VARCHAR(20) DEFAULT 'VALID',
    dq_issues                VARIANT,
    dq_issue_count           NUMBER DEFAULT 0,
    has_dq_issues            BOOLEAN DEFAULT FALSE,
    
    -- Audit
    raw_record_id            NUMBER,
    batch_id                 VARCHAR(100),
    record_hash              VARCHAR(64),
    ingestion_timestamp      TIMESTAMP_NTZ,
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by               VARCHAR(100) DEFAULT CURRENT_USER(),
    
    -- Constraints
    CONSTRAINT pk_fact_transaction PRIMARY KEY (transaction_sk),
    CONSTRAINT uk_fact_transaction UNIQUE (transaction_id, source_client)
)
CLUSTER BY (order_date, source_client)
COMMENT = 'Transaction fact table - one row per transaction';

-- ============================================================================
-- FACT_TRANSACTION_ITEM (Line Level)
-- Grain: One row per line item per transaction
-- ============================================================================
CREATE OR REPLACE TABLE canonical.fact_transaction_item (
    -- Surrogate Key
    transaction_item_sk      NUMBER AUTOINCREMENT,
    
    -- Foreign Keys
    transaction_sk           NUMBER NOT NULL,
    product_sk               NUMBER DEFAULT -1,
    order_date_sk            NUMBER DEFAULT -1,
    
    -- Business Keys
    transaction_id           VARCHAR(100) NOT NULL,
    line_number              NUMBER NOT NULL,
    
    -- Item Identifiers
    sku                      VARCHAR(100),
    
    -- Measures
    quantity                 NUMBER,
    quantity_abs             NUMBER,
    unit_price               DECIMAL(18,4),
    unit_price_abs           DECIMAL(18,4),
    line_total               DECIMAL(18,4),
    line_total_abs           DECIMAL(18,4),
    currency                 VARCHAR(10) DEFAULT 'USD',
    
    -- Descriptive
    description              VARCHAR(2000),
    
    -- Metadata (for extra fields)
    item_metadata            VARIANT,
    
    -- Data Quality
    dq_status                VARCHAR(20) DEFAULT 'VALID',
    has_negative_quantity    BOOLEAN DEFAULT FALSE,
    has_negative_price       BOOLEAN DEFAULT FALSE,
    is_missing_sku           BOOLEAN DEFAULT FALSE,
    is_missing_description   BOOLEAN DEFAULT FALSE,
    
    -- Source Tracking
    source_file_name         VARCHAR(500),
    source_client            VARCHAR(50),
    
    -- ADD batch_id for lineage
    batch_id                 VARCHAR(100),
    
    -- Audit
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_fact_transaction_item PRIMARY KEY (transaction_item_sk),
    CONSTRAINT fk_fact_txn_item_txn FOREIGN KEY (transaction_sk) REFERENCES canonical.fact_transaction(transaction_sk),
    CONSTRAINT uk_fact_transaction_item UNIQUE (transaction_id, line_number, source_client)
)
CLUSTER BY (transaction_sk)
COMMENT = 'Transaction line items - one row per item';

-- ============================================================================
-- FACT_ORDER (From CSV Reference Data)
-- Grain: One row per order
-- ============================================================================
CREATE OR REPLACE TABLE canonical.fact_order (
    -- Surrogate Key
    order_sk                 NUMBER DEFAULT canonical.seq_order_sk.NEXTVAL,
    
    -- Business Key
    order_id                 VARCHAR(100) NOT NULL,
    
    -- Foreign Keys
    customer_sk              NUMBER DEFAULT -1,
    order_date_sk            NUMBER DEFAULT -1,
    order_status_sk          NUMBER,
    
    -- Degenerate Dimensions
    customer_id              VARCHAR(100),
    order_date               DATE,
    
    -- Measures
    total_amount             DECIMAL(18,4),
    currency                 VARCHAR(10) DEFAULT 'USD',
    
    -- Status
    status                   VARCHAR(50),
    
    -- Source Tracking
    source_file_name         VARCHAR(500),
    
    -- Data Quality
    dq_status                VARCHAR(20) DEFAULT 'VALID',
    has_dq_issues            BOOLEAN DEFAULT FALSE,
    
    -- Audit
    raw_record_id            NUMBER,
    batch_id                 VARCHAR(100),
    record_hash              VARCHAR(64),
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_fact_order PRIMARY KEY (order_sk),
    CONSTRAINT uk_fact_order UNIQUE (order_id)
)
COMMENT = 'Order fact from CSV reference data';

-- ============================================================================
-- FACT_PAYMENT (From CSV Reference Data)
-- Grain: One row per payment
-- ============================================================================
CREATE OR REPLACE TABLE canonical.fact_payment (
    -- Surrogate Key
    payment_sk               NUMBER DEFAULT canonical.seq_payment_sk.NEXTVAL,
    
    -- Business Keys
    payment_id               VARCHAR(100) NOT NULL,
    order_id                 VARCHAR(100),
    transaction_id           VARCHAR(100),
    
    -- Foreign Keys
    payment_method_sk        NUMBER,
    payment_date_sk          NUMBER DEFAULT -1,
    
    -- Measures
    amount                   DECIMAL(18,4),
    amount_abs               DECIMAL(18,4),
    currency                 VARCHAR(10) DEFAULT 'USD',
    
    -- Status
    status                   VARCHAR(50),
    payment_date             DATE,
    
    -- Source Tracking
    source_file_name         VARCHAR(500),
    
    -- Data Quality
    dq_status                VARCHAR(20) DEFAULT 'VALID',
    is_negative_amount       BOOLEAN DEFAULT FALSE,
    
    -- Audit
    raw_record_id            NUMBER,
    batch_id                 VARCHAR(100),
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_fact_payment PRIMARY KEY (payment_sk),
    CONSTRAINT uk_fact_payment UNIQUE (payment_id)
)
COMMENT = 'Payment fact from CSV reference data';

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'FACT TABLES CREATED' AS status;

SELECT 
    TABLE_NAME,
    ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CANONICAL'
  AND TABLE_NAME LIKE 'FACT_%'
ORDER BY TABLE_NAME;

/*
================================================================================
CANONICAL: DIMENSION TABLES
================================================================================
Purpose: Create unified dimensional model (Kimball-style star schema)
Author:  Data Platform Team
Version: 2.0

Dimensions:
- dim_customer: Customer dimension (SCD Type 2 ready)
- dim_product: Product dimension
- dim_date: Date dimension (pre-populated)
- dim_payment_method: Payment method reference
- dim_order_status: Order status reference
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA canonical;

-- ============================================================================
-- DIM_CUSTOMER (SCD Type 2)
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_customer (
    -- Surrogate Key
    customer_sk              NUMBER DEFAULT canonical.seq_customer_sk.NEXTVAL,
    
    -- Business Key
    customer_id              VARCHAR(100) NOT NULL,
    
    -- Attributes
    first_name               VARCHAR(200),
    last_name                VARCHAR(200),
    full_name                VARCHAR(400) AS (CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))),
    email                    VARCHAR(500),
    email_domain             VARCHAR(200),
    email_validation_status  VARCHAR(20),
    loyalty_tier             VARCHAR(50),
    signup_source            VARCHAR(100),
    is_active                BOOLEAN,
    
    -- Source Tracking
    source_client            VARCHAR(50),
    source_file_name         VARCHAR(500),
    source_system            VARCHAR(100) DEFAULT 'FINANCIAL_DATA_PLATFORM',
    
    -- SCD Type 2 Columns
    effective_from           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to             TIMESTAMP_NTZ DEFAULT '9999-12-31 23:59:59',
    is_current               BOOLEAN DEFAULT TRUE,
    version_number           NUMBER DEFAULT 1,
    
    -- Audit
    record_hash              VARCHAR(64),
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by               VARCHAR(100) DEFAULT CURRENT_USER(),
    updated_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by               VARCHAR(100) DEFAULT CURRENT_USER(),
    
    -- Constraints
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT uk_dim_customer_bk UNIQUE (customer_id, effective_from)
)
CLUSTER BY (customer_id, is_current)
COMMENT = 'Customer dimension - SCD Type 2 enabled for historical tracking';

-- Unknown member for referential integrity
INSERT INTO canonical.dim_customer (
    customer_sk, customer_id, first_name, last_name, email, 
    email_validation_status, loyalty_tier, source_client
) VALUES (
    -1, 'UNKNOWN', 'Unknown', 'Customer', 'unknown@unknown.com',
    'UNKNOWN', 'UNKNOWN', 'SYSTEM'
);

-- ============================================================================
-- DIM_PRODUCT (SCD Type 2)
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_product (
    -- Surrogate Key
    product_sk               NUMBER DEFAULT canonical.seq_product_sk.NEXTVAL,
    
    -- Business Key
    sku                      VARCHAR(100) NOT NULL,
    
    -- Attributes
    product_id               VARCHAR(100),
    product_name             VARCHAR(500),
    description              VARCHAR(2000),
    category                 VARCHAR(200),
    subcategory              VARCHAR(200),
    price                    DECIMAL(18,4),
    currency                 VARCHAR(10) DEFAULT 'USD',
    
    -- Source Tracking
    source_client            VARCHAR(50),
    source_file_name         VARCHAR(500),
    
    -- SCD Type 2 Columns
    effective_from           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to             TIMESTAMP_NTZ DEFAULT '9999-12-31 23:59:59',
    is_current               BOOLEAN DEFAULT TRUE,
    version_number           NUMBER DEFAULT 1,
    
    -- Audit
    record_hash              VARCHAR(64),
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk),
    CONSTRAINT uk_dim_product_bk UNIQUE (sku, effective_from)
)
CLUSTER BY (sku, is_current)
COMMENT = 'Product dimension - SKU as business key';

-- Unknown member
INSERT INTO canonical.dim_product (
    product_sk, sku, product_name, description, category, source_client
) VALUES (
    -1, 'UNKNOWN', 'Unknown Product', 'Unknown product placeholder', 'Unknown', 'SYSTEM'
);

-- ============================================================================
-- DIM_DATE
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_date (
    -- Keys
    date_sk                  NUMBER NOT NULL,
    date_actual              DATE NOT NULL,
    
    -- Day Attributes
    day_of_week              NUMBER,
    day_of_week_name         VARCHAR(10),
    day_of_week_short        VARCHAR(3),
    day_of_month             NUMBER,
    day_of_year              NUMBER,
    is_weekday               BOOLEAN,
    is_weekend               BOOLEAN,
    
    -- Week Attributes
    week_of_year             NUMBER,
    week_of_month            NUMBER,
    week_start_date          DATE,
    week_end_date            DATE,
    
    -- Month Attributes
    month_actual             NUMBER,
    month_name               VARCHAR(10),
    month_name_short         VARCHAR(3),
    month_start_date         DATE,
    month_end_date           DATE,
    
    -- Quarter Attributes
    quarter_actual           NUMBER,
    quarter_name             VARCHAR(2),
    quarter_start_date       DATE,
    quarter_end_date         DATE,
    
    -- Year Attributes
    year_actual              NUMBER,
    year_start_date          DATE,
    year_end_date            DATE,
    
    -- Fiscal (assuming calendar year = fiscal year)
    fiscal_year              NUMBER,
    fiscal_quarter           NUMBER,
    
    -- Flags
    is_holiday               BOOLEAN DEFAULT FALSE,
    holiday_name             VARCHAR(100),
    is_last_day_of_month     BOOLEAN,
    is_last_day_of_quarter   BOOLEAN,
    is_last_day_of_year      BOOLEAN,
    
    -- Constraints
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
)
COMMENT = 'Date dimension for time-based analytics';

-- Populate dim_date (2020-2030)
INSERT INTO canonical.dim_date
WITH date_spine AS (
    SELECT DATEADD(DAY, seq4(), '2020-01-01'::DATE) AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- ~11 years
)
SELECT
    TO_NUMBER(TO_CHAR(d.date_val, 'YYYYMMDD')) AS date_sk,
    d.date_val AS date_actual,
    
    -- Day
    DAYOFWEEK(d.date_val) AS day_of_week,
    DAYNAME(d.date_val) AS day_of_week_name,
    LEFT(DAYNAME(d.date_val), 3) AS day_of_week_short,
    DAY(d.date_val) AS day_of_month,
    DAYOFYEAR(d.date_val) AS day_of_year,
    CASE WHEN DAYOFWEEK(d.date_val) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
    CASE WHEN DAYOFWEEK(d.date_val) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    
    -- Week
    WEEKOFYEAR(d.date_val) AS week_of_year,
    CEIL(DAY(d.date_val) / 7.0) AS week_of_month,
    DATE_TRUNC('WEEK', d.date_val)::DATE AS week_start_date,
    DATEADD(DAY, 6, DATE_TRUNC('WEEK', d.date_val))::DATE AS week_end_date,
    
    -- Month
    MONTH(d.date_val) AS month_actual,
    MONTHNAME(d.date_val) AS month_name,
    LEFT(MONTHNAME(d.date_val), 3) AS month_name_short,
    DATE_TRUNC('MONTH', d.date_val)::DATE AS month_start_date,
    LAST_DAY(d.date_val)::DATE AS month_end_date,
    
    -- Quarter
    QUARTER(d.date_val) AS quarter_actual,
    'Q' || QUARTER(d.date_val) AS quarter_name,
    DATE_TRUNC('QUARTER', d.date_val)::DATE AS quarter_start_date,
    LAST_DAY(DATEADD(MONTH, 2, DATE_TRUNC('QUARTER', d.date_val)))::DATE AS quarter_end_date,
    
    -- Year
    YEAR(d.date_val) AS year_actual,
    DATE_TRUNC('YEAR', d.date_val)::DATE AS year_start_date,
    LAST_DAY(DATEADD(MONTH, 11, DATE_TRUNC('YEAR', d.date_val)))::DATE AS year_end_date,
    
    -- Fiscal
    YEAR(d.date_val) AS fiscal_year,
    QUARTER(d.date_val) AS fiscal_quarter,
    
    -- Flags
    FALSE AS is_holiday,
    NULL AS holiday_name,
    d.date_val = LAST_DAY(d.date_val) AS is_last_day_of_month,
    d.date_val = LAST_DAY(DATEADD(MONTH, 2, DATE_TRUNC('QUARTER', d.date_val))) AS is_last_day_of_quarter,
    d.date_val = LAST_DAY(DATEADD(MONTH, 11, DATE_TRUNC('YEAR', d.date_val))) AS is_last_day_of_year

FROM date_spine d
WHERE NOT EXISTS (SELECT 1 FROM canonical.dim_date WHERE date_actual = d.date_val);

-- Unknown date member
INSERT INTO canonical.dim_date (date_sk, date_actual, day_of_week, month_actual, year_actual)
SELECT -1, '1900-01-01'::DATE, 0, 1, 1900
WHERE NOT EXISTS (SELECT 1 FROM canonical.dim_date WHERE date_sk = -1);

-- ============================================================================
-- DIM_PAYMENT_METHOD
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_payment_method (
    payment_method_sk        NUMBER AUTOINCREMENT,
    payment_method_code      VARCHAR(50) NOT NULL,
    payment_method_name      VARCHAR(100),
    payment_method_category  VARCHAR(50),
    is_digital               BOOLEAN,
    is_active                BOOLEAN DEFAULT TRUE,
    display_order            NUMBER,
    
    CONSTRAINT pk_dim_payment_method PRIMARY KEY (payment_method_sk),
    CONSTRAINT uk_dim_payment_method UNIQUE (payment_method_code)
)
COMMENT = 'Payment method reference dimension';

-- Seed payment methods
INSERT INTO canonical.dim_payment_method (payment_method_code, payment_method_name, payment_method_category, is_digital, display_order)
VALUES 
    ('CREDIT_CARD', 'Credit Card', 'Card', FALSE, 1),
    ('DEBIT_CARD', 'Debit Card', 'Card', FALSE, 2),
    ('PAYPAL', 'PayPal', 'Digital Wallet', TRUE, 3),
    ('BANK_TRANSFER', 'Bank Transfer', 'Bank', FALSE, 4),
    ('OTHER', 'Other', 'Other', NULL, 98),
    ('MISSING', 'Missing/Unknown', 'Unknown', NULL, 99),
    ('UNKNOWN', 'Unknown', 'Unknown', NULL, 100);

-- ============================================================================
-- DIM_ORDER_STATUS
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_order_status (
    order_status_sk          NUMBER AUTOINCREMENT,
    status_code              VARCHAR(50) NOT NULL,
    status_name              VARCHAR(100),
    status_category          VARCHAR(50),
    is_terminal              BOOLEAN,
    display_order            NUMBER,
    
    CONSTRAINT pk_dim_order_status PRIMARY KEY (order_status_sk),
    CONSTRAINT uk_dim_order_status UNIQUE (status_code)
)
COMMENT = 'Order status reference dimension';

-- Seed order statuses
INSERT INTO canonical.dim_order_status (status_code, status_name, status_category, is_terminal, display_order)
VALUES 
    ('PENDING', 'Pending', 'Open', FALSE, 1),
    ('PROCESSING', 'Processing', 'Open', FALSE, 2),
    ('SHIPPED', 'Shipped', 'In Transit', FALSE, 3),
    ('DELIVERED', 'Delivered', 'Closed', TRUE, 4),
    ('COMPLETED', 'Completed', 'Closed', TRUE, 5),
    ('CANCELLED', 'Cancelled', 'Closed', TRUE, 6),
    ('REFUNDED', 'Refunded', 'Closed', TRUE, 7),
    ('UNKNOWN', 'Unknown', 'Unknown', NULL, 99);

-- ============================================================================
-- DIM_CURRENCY
-- ============================================================================
CREATE OR REPLACE TABLE canonical.dim_currency (
    currency_sk              NUMBER AUTOINCREMENT,
    currency_code            VARCHAR(10) NOT NULL,
    currency_name            VARCHAR(100),
    currency_symbol          VARCHAR(10),
    is_active                BOOLEAN DEFAULT TRUE,
    
    CONSTRAINT pk_dim_currency PRIMARY KEY (currency_sk),
    CONSTRAINT uk_dim_currency UNIQUE (currency_code)
)
COMMENT = 'Currency reference dimension';

-- Seed currencies
INSERT INTO canonical.dim_currency (currency_code, currency_name, currency_symbol)
VALUES 
    ('USD', 'US Dollar', '$'),
    ('EUR', 'Euro', '€'),
    ('GBP', 'British Pound', '£'),
    ('UNKNOWN', 'Unknown', '?');

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'DIMENSIONS CREATED' AS status;

SELECT 
    TABLE_NAME,
    ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CANONICAL'
  AND TABLE_NAME LIKE 'DIM_%'
ORDER BY TABLE_NAME;

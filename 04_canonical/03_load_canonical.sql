/*
================================================================================
CANONICAL: LOAD PROCEDURES - COMPLETE
================================================================================
Purpose: Load data from staging to canonical tables
Author:  Data Platform Team
Version: 2.0
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA canonical;

-- ============================================================================
-- PROCEDURE: Load Dimension Customers
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_dim_customer()
RETURNS TABLE (action VARCHAR, row_count NUMBER)
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER := 0;
    rows_inserted NUMBER := 0;
BEGIN
    -- Step 1: Load from CSV reference data (priority source)
    MERGE INTO canonical.dim_customer tgt
    USING (
        SELECT DISTINCT
            customer_id,
            first_name,
            last_name,
            email,
            SPLIT_PART(email, '@', 2) AS email_domain,
            email_validation_status,
            loyalty_tier,
            signup_source,
            is_active,
            'CSV' AS source_client,
            source_file_name,
            record_hash
        FROM staging.v_stg_customers
        WHERE customer_id IS NOT NULL
          AND is_missing_customer_id = FALSE
    ) src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    WHEN MATCHED AND (tgt.record_hash IS NULL OR tgt.record_hash != src.record_hash) THEN
        UPDATE SET
            tgt.effective_to = CURRENT_TIMESTAMP(),
            tgt.is_current = FALSE,
            tgt.updated_at = CURRENT_TIMESTAMP(),
            tgt.updated_by = CURRENT_USER()
    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, email, email_domain, 
                email_validation_status, loyalty_tier, signup_source, is_active,
                source_client, source_file_name, record_hash)
        VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.email_domain,
                src.email_validation_status, src.loyalty_tier, src.signup_source, src.is_active,
                src.source_client, src.source_file_name, src.record_hash);
    
    rows_merged := SQLROWCOUNT;
    
    -- Step 2: Load new customers from ClientA transactions
    INSERT INTO canonical.dim_customer (
        customer_id, first_name, last_name, email, email_domain,
        email_validation_status, loyalty_tier, source_client, source_file_name
    )
    SELECT DISTINCT
        customer_id,
        first_name,
        last_name,
        email,
        SPLIT_PART(email, '@', 2) AS email_domain,
        email_validation_status,
        loyalty_tier_extra AS loyalty_tier,
        source_client,
        source_file_name
    FROM staging.v_stg_client_a_transactions
    WHERE customer_id IS NOT NULL
      AND is_missing_customer_id = FALSE
      AND customer_id NOT IN (
          SELECT customer_id FROM canonical.dim_customer WHERE is_current = TRUE
      );
    
    rows_inserted := rows_inserted + SQLROWCOUNT;
    
    -- Step 3: Load new customers from ClientC transactions
    INSERT INTO canonical.dim_customer (
        customer_id, first_name, last_name, email, email_domain,
        email_validation_status, loyalty_tier, source_client, source_file_name
    )
    SELECT DISTINCT
        customer_id,
        first_name,
        last_name,
        email,
        SPLIT_PART(email, '@', 2) AS email_domain,
        email_validation_status,
        loyalty_tier,
        source_client,
        source_file_name
    FROM staging.v_stg_client_c_transactions
    WHERE customer_id IS NOT NULL
      AND is_missing_customer_id = FALSE
      AND customer_id NOT IN (
          SELECT customer_id FROM canonical.dim_customer WHERE is_current = TRUE
      );
    
    rows_inserted := rows_inserted + SQLROWCOUNT;
    
    RETURN TABLE(
        SELECT 'MERGED' AS action, rows_merged AS row_count
        UNION ALL
        SELECT 'INSERTED', rows_inserted
    );
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Dimension Products
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_dim_product()
RETURNS TABLE (action VARCHAR, row_count NUMBER)
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER := 0;
    rows_inserted NUMBER := 0;
BEGIN
    -- Step 1: Load from CSV reference data
    MERGE INTO canonical.dim_product tgt
    USING (
        SELECT DISTINCT
            product_id,
            sku,
            product_name,
            description,
            category,
            price,
            currency,
            'CSV' AS source_client,
            source_file_name,
            record_hash
        FROM staging.v_stg_products
        WHERE sku IS NOT NULL
          AND is_missing_sku = FALSE
    ) src
    ON tgt.sku = src.sku AND tgt.is_current = TRUE
    WHEN MATCHED AND (tgt.record_hash IS NULL OR tgt.record_hash != src.record_hash) THEN
        UPDATE SET
            tgt.effective_to = CURRENT_TIMESTAMP(),
            tgt.is_current = FALSE,
            tgt.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (product_id, sku, product_name, description, category, price, currency,
                source_client, source_file_name, record_hash)
        VALUES (src.product_id, src.sku, src.product_name, src.description, src.category,
                src.price, src.currency, src.source_client, src.source_file_name, src.record_hash);
    
    rows_merged := SQLROWCOUNT;
    
    -- Step 2: Load products from ClientA transaction items
    INSERT INTO canonical.dim_product (sku, product_name, description, source_client, source_file_name)
    SELECT DISTINCT
        sku,
        description AS product_name,
        description,
        'ClientA' AS source_client,
        source_file_name
    FROM staging.v_stg_client_a_transaction_items
    WHERE sku IS NOT NULL
      AND is_missing_sku = FALSE
      AND sku NOT IN (SELECT sku FROM canonical.dim_product WHERE is_current = TRUE);
    
    rows_inserted := rows_inserted + SQLROWCOUNT;
    
    -- Step 3: Load products from ClientC transaction items
    INSERT INTO canonical.dim_product (sku, product_name, description, source_client, source_file_name)
    SELECT DISTINCT
        sku,
        description AS product_name,
        description,
        'ClientC' AS source_client,
        source_file_name
    FROM staging.v_stg_client_c_transaction_items
    WHERE sku IS NOT NULL
      AND is_missing_sku = FALSE
      AND sku NOT IN (SELECT sku FROM canonical.dim_product WHERE is_current = TRUE);
    
    rows_inserted := rows_inserted + SQLROWCOUNT;
    
    RETURN TABLE(
        SELECT 'MERGED' AS action, rows_merged AS row_count
        UNION ALL
        SELECT 'INSERTED', rows_inserted
    );
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Fact Transactions (ClientA)
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_fact_transaction_client_a()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted NUMBER := 0;
BEGIN
    INSERT INTO canonical.fact_transaction (
        transaction_id, order_id, customer_sk, order_date_sk, payment_method_sk,
        customer_id, order_date, payment_amount, payment_amount_abs, payment_currency,
        source_client, source_file_name, source_format, dq_status,
        dq_issues, dq_issue_count, has_dq_issues, raw_record_id, batch_id, ingestion_timestamp
    )
    SELECT
        t.transaction_id,
        t.order_id,
        COALESCE(c.customer_sk, -1) AS customer_sk,
        COALESCE(d.date_sk, -1) AS order_date_sk,
        COALESCE(pm.payment_method_sk, (SELECT payment_method_sk FROM canonical.dim_payment_method WHERE payment_method_code = 'UNKNOWN')) AS payment_method_sk,
        t.customer_id,
        t.order_date,
        t.payment_amount,
        ABS(t.payment_amount) AS payment_amount_abs,
        t.payment_currency,
        t.source_client,
        t.source_file_name,
        t.source_format,
        CASE 
            WHEN t.is_missing_transaction_id THEN 'CRITICAL'
            WHEN t.is_missing_order_id OR t.is_missing_customer_id OR t.is_negative_payment_amount THEN 'INVALID'
            WHEN t.is_missing_email OR t.is_missing_order_date OR t.is_missing_payment_method THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        OBJECT_CONSTRUCT(
            'is_missing_transaction_id', t.is_missing_transaction_id,
            'is_missing_order_id', t.is_missing_order_id,
            'is_missing_customer_id', t.is_missing_customer_id,
            'is_missing_email', t.is_missing_email,
            'is_missing_order_date', t.is_missing_order_date,
            'is_missing_payment_method', t.is_missing_payment_method,
            'is_missing_payment_amount', t.is_missing_payment_amount,
            'is_negative_payment_amount', t.is_negative_payment_amount,
            'email_validation_status', t.email_validation_status
        ) AS dq_issues,
        (CASE WHEN t.is_missing_transaction_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_order_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_customer_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_email THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_order_date THEN 1 ELSE 0 END +
         CASE WHEN t.is_negative_payment_amount THEN 1 ELSE 0 END) AS dq_issue_count,
        (t.is_missing_transaction_id OR t.is_missing_order_id OR t.is_missing_customer_id OR 
         t.is_missing_email OR t.is_missing_order_date OR t.is_negative_payment_amount) AS has_dq_issues,
        t.raw_record_id,
        t.batch_id,
        t.ingestion_timestamp
    FROM staging.v_stg_client_a_transactions t
    LEFT JOIN canonical.dim_customer c 
        ON t.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN canonical.dim_date d 
        ON t.order_date = d.date_actual
    LEFT JOIN canonical.dim_payment_method pm 
        ON t.payment_method = pm.payment_method_code
    WHERE t.transaction_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM canonical.fact_transaction f 
          WHERE f.transaction_id = t.transaction_id 
            AND f.source_client = 'ClientA'
      );
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'ClientA transactions loaded: ' || rows_inserted || ' rows';
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Fact Transactions (ClientC)
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_fact_transaction_client_c()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted NUMBER := 0;
BEGIN
    INSERT INTO canonical.fact_transaction (
        transaction_id, order_id, customer_sk, order_date_sk, payment_method_sk,
        customer_id, order_date, payment_amount, payment_amount_abs, payment_currency,
        source_client, source_file_name, source_format, dq_status,
        dq_issues, dq_issue_count, has_dq_issues, raw_record_id, batch_id, ingestion_timestamp
    )
    SELECT
        t.transaction_id,
        t.order_id,
        COALESCE(c.customer_sk, -1) AS customer_sk,
        COALESCE(d.date_sk, -1) AS order_date_sk,
        COALESCE(pm.payment_method_sk, (SELECT payment_method_sk FROM canonical.dim_payment_method WHERE payment_method_code = 'UNKNOWN')) AS payment_method_sk,
        t.customer_id,
        t.order_date,
        t.payment_amount,
        t.payment_amount_abs,
        t.payment_currency,
        t.source_client,
        t.source_file_name,
        t.source_format,
        CASE 
            WHEN t.is_missing_transaction_id THEN 'CRITICAL'
            WHEN t.is_missing_order_id OR t.is_missing_customer_id OR t.is_negative_payment_amount THEN 'INVALID'
            WHEN t.is_missing_email OR t.is_missing_order_date OR t.is_missing_payment_method THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        OBJECT_CONSTRUCT(
            'is_missing_transaction_id', t.is_missing_transaction_id,
            'is_missing_order_id', t.is_missing_order_id,
            'is_missing_customer_id', t.is_missing_customer_id,
            'is_missing_email', t.is_missing_email,
            'is_missing_order_date', t.is_missing_order_date,
            'is_negative_payment_amount', t.is_negative_payment_amount,
            'email_validation_status', t.email_validation_status
        ) AS dq_issues,
        (CASE WHEN t.is_missing_transaction_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_order_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_customer_id THEN 1 ELSE 0 END +
         CASE WHEN t.is_missing_email THEN 1 ELSE 0 END +
         CASE WHEN t.is_negative_payment_amount THEN 1 ELSE 0 END) AS dq_issue_count,
        (t.is_missing_transaction_id OR t.is_missing_order_id OR t.is_missing_customer_id OR 
         t.is_missing_email OR t.is_negative_payment_amount) AS has_dq_issues,
        t.raw_record_id,
        t.batch_id,
        t.ingestion_timestamp
    FROM staging.v_stg_client_c_transactions t
    LEFT JOIN canonical.dim_customer c 
        ON t.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN canonical.dim_date d 
        ON t.order_date = d.date_actual
    LEFT JOIN canonical.dim_payment_method pm 
        ON t.payment_method = pm.payment_method_code
    WHERE t.transaction_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM canonical.fact_transaction f 
          WHERE f.transaction_id = t.transaction_id 
            AND f.source_client = 'ClientC'
      );
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'ClientC transactions loaded: ' || rows_inserted || ' rows';
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Fact Transaction Items
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_fact_transaction_item()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_client_a NUMBER := 0;
    rows_client_c NUMBER := 0;
BEGIN
    -- Load ClientA items
    INSERT INTO canonical.fact_transaction_item (
        transaction_sk, product_sk, order_date_sk, transaction_id, line_number,
        sku, quantity, quantity_abs, unit_price, unit_price_abs, line_total,
        line_total_abs, currency, description, item_metadata, dq_status,
        has_negative_quantity, has_negative_price, is_missing_sku, is_missing_description,
        source_file_name, source_client
    )
    SELECT
        ft.transaction_sk,
        COALESCE(p.product_sk, -1) AS product_sk,
        ft.order_date_sk,
        i.transaction_id,
        i.line_number,
        i.sku,
        i.quantity,
        i.quantity_abs,
        i.unit_price,
        i.unit_price_abs,
        i.line_total,
        COALESCE(ABS(i.line_total), i.quantity_abs * i.unit_price_abs) AS line_total_abs,
        i.currency,
        i.description,
        OBJECT_CONSTRUCT(
            'attributes', i.attributes_extra,
            'warranty', i.warranty_extra,
            'gift_options', i.gift_options_extra
        ) AS item_metadata,
        CASE 
            WHEN i.is_missing_sku THEN 'INVALID'
            WHEN i.is_negative_quantity OR i.is_negative_unit_price THEN 'WARNING'
            WHEN i.is_missing_description THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        i.is_negative_quantity,
        i.is_negative_unit_price,
        i.is_missing_sku,
        i.is_missing_description,
        i.source_file_name,
        'ClientA' AS source_client
    FROM staging.v_stg_client_a_transaction_items i
    JOIN canonical.fact_transaction ft 
        ON i.transaction_id = ft.transaction_id 
        AND ft.source_client = 'ClientA'
    LEFT JOIN canonical.dim_product p 
        ON i.sku = p.sku AND p.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM canonical.fact_transaction_item fi
        WHERE fi.transaction_id = i.transaction_id
          AND fi.line_number = i.line_number
          AND fi.source_client = 'ClientA'
    );
    
    rows_client_a := SQLROWCOUNT;
    
    -- Load ClientC items
    INSERT INTO canonical.fact_transaction_item (
        transaction_sk, product_sk, order_date_sk, transaction_id, line_number,
        sku, quantity, quantity_abs, unit_price, unit_price_abs, line_total,
        line_total_abs, currency, description, item_metadata, dq_status,
        has_negative_quantity, has_negative_price, is_missing_sku, is_missing_description,
        source_file_name, source_client
    )
    SELECT
        ft.transaction_sk,
        COALESCE(p.product_sk, -1) AS product_sk,
        ft.order_date_sk,
        i.transaction_id,
        i.line_number,
        i.sku,
        i.quantity,
        i.quantity_abs,
        i.unit_price,
        i.unit_price_abs,
        i.line_total,
        COALESCE(ABS(i.line_total), i.quantity_abs * i.unit_price_abs) AS line_total_abs,
        i.currency,
        i.description,
        i.item_metadata_json AS item_metadata,
        CASE 
            WHEN i.is_missing_sku THEN 'INVALID'
            WHEN i.is_negative_quantity OR i.is_negative_unit_price THEN 'WARNING'
            WHEN i.is_missing_description THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        i.is_negative_quantity,
        i.is_negative_unit_price,
        i.is_missing_sku,
        i.is_missing_description,
        i.source_file_name,
        'ClientC' AS source_client
    FROM staging.v_stg_client_c_transaction_items i
    JOIN canonical.fact_transaction ft 
        ON i.transaction_id = ft.transaction_id 
        AND ft.source_client = 'ClientC'
    LEFT JOIN canonical.dim_product p 
        ON i.sku = p.sku AND p.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM canonical.fact_transaction_item fi
        WHERE fi.transaction_id = i.transaction_id
          AND fi.line_number = i.line_number
          AND fi.source_client = 'ClientC'
    );
    
    rows_client_c := SQLROWCOUNT;
    
    RETURN 'Transaction items loaded - ClientA: ' || rows_client_a || ', ClientC: ' || rows_client_c;
END;
$$;

-- ============================================================================
-- PROCEDURE: Update Transaction Aggregates
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_update_transaction_aggregates()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE canonical.fact_transaction ft
    SET 
        item_count = agg.item_count,
        total_line_amount = agg.total_line_amount,
        total_line_amount_abs = agg.total_line_amount_abs,
        amount_variance = ft.payment_amount_abs - agg.total_line_amount_abs,
        has_amount_variance = ABS(COALESCE(ft.payment_amount_abs, 0) - COALESCE(agg.total_line_amount_abs, 0)) > 0.01
    FROM (
        SELECT 
            transaction_sk,
            COUNT(*) AS item_count,
            SUM(line_total) AS total_line_amount,
            SUM(line_total_abs) AS total_line_amount_abs
        FROM canonical.fact_transaction_item
        GROUP BY transaction_sk
    ) agg
    WHERE ft.transaction_sk = agg.transaction_sk
      AND (ft.item_count IS NULL OR ft.item_count != agg.item_count
           OR ft.total_line_amount IS NULL OR ft.total_line_amount != agg.total_line_amount);
    
    RETURN 'Transaction aggregates updated: ' || SQLROWCOUNT || ' rows';
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Fact Orders from CSV
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_fact_order()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO canonical.fact_order (
        order_id, customer_sk, order_date_sk, order_status_sk,
        customer_id, order_date, total_amount, currency, status,
        source_file_name, dq_status, has_dq_issues, raw_record_id, batch_id, record_hash
    )
    SELECT
        o.order_id,
        COALESCE(c.customer_sk, -1) AS customer_sk,
        COALESCE(d.date_sk, -1) AS order_date_sk,
        os.order_status_sk,
        o.customer_id,
        o.order_date,
        o.total_amount,
        o.currency,
        o.status,
        o.source_file_name,
        CASE 
            WHEN o.is_missing_order_id THEN 'INVALID'
            WHEN o.is_missing_customer_id OR o.is_invalid_order_date THEN 'WARNING'
            WHEN o.is_negative_amount THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        (o.is_missing_order_id OR o.is_missing_customer_id OR o.is_negative_amount) AS has_dq_issues,
        o.raw_record_id,
        o.batch_id,
        o.record_hash
    FROM staging.v_stg_orders o
    LEFT JOIN canonical.dim_customer c 
        ON o.customer_id = c.customer_id AND c.is_current = TRUE
    LEFT JOIN canonical.dim_date d 
        ON o.order_date = d.date_actual
    LEFT JOIN canonical.dim_order_status os 
        ON UPPER(o.status) = os.status_code
    WHERE o.order_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM canonical.fact_order f WHERE f.order_id = o.order_id);
    
    RETURN 'Orders loaded: ' || SQLROWCOUNT || ' rows';
END;
$$;

-- ============================================================================
-- PROCEDURE: Load Fact Payments from CSV
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_fact_payment()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO canonical.fact_payment (
        payment_id, order_id, transaction_id, payment_method_sk, payment_date_sk,
        amount, amount_abs, currency, status, payment_date,
        source_file_name, dq_status, is_negative_amount, raw_record_id, batch_id
    )
    SELECT
        p.payment_id,
        p.order_id,
        p.transaction_id,
        COALESCE(pm.payment_method_sk, (SELECT payment_method_sk FROM canonical.dim_payment_method WHERE payment_method_code = 'UNKNOWN')) AS payment_method_sk,
        COALESCE(d.date_sk, -1) AS payment_date_sk,
        p.amount,
        ABS(p.amount) AS amount_abs,
        p.currency,
        p.status,
        p.payment_date,
        p.source_file_name,
        CASE 
            WHEN p.is_missing_payment_id THEN 'INVALID'
            WHEN p.is_negative_amount THEN 'WARNING'
            ELSE 'VALID'
        END AS dq_status,
        p.is_negative_amount,
        p.raw_record_id,
        p.batch_id
    FROM staging.v_stg_payments p
    LEFT JOIN canonical.dim_payment_method pm 
        ON p.payment_method = pm.payment_method_code
    LEFT JOIN canonical.dim_date d 
        ON p.payment_date = d.date_actual
    WHERE p.payment_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM canonical.fact_payment f WHERE f.payment_id = p.payment_id);
    
    RETURN 'Payments loaded: ' || SQLROWCOUNT || ' rows';
END;
$$;

-- ============================================================================
-- MASTER LOAD PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE canonical.sp_load_all()
RETURNS TABLE (step VARCHAR, result VARCHAR, completed_at TIMESTAMP_NTZ)
LANGUAGE SQL
AS
$$
DECLARE
    result_customer VARCHAR;
    result_product VARCHAR;
    result_txn_a VARCHAR;
    result_txn_c VARCHAR;
    result_items VARCHAR;
    result_agg VARCHAR;
    result_orders VARCHAR;
    result_payments VARCHAR;
BEGIN
    -- Load dimensions first
    CALL canonical.sp_load_dim_customer();
    CALL canonical.sp_load_dim_product();
    
    -- Load facts
    CALL canonical.sp_load_fact_transaction_client_a() INTO result_txn_a;
    CALL canonical.sp_load_fact_transaction_client_c() INTO result_txn_c;
    CALL canonical.sp_load_fact_transaction_item() INTO result_items;
    CALL canonical.sp_update_transaction_aggregates() INTO result_agg;
    CALL canonical.sp_load_fact_order() INTO result_orders;
    CALL canonical.sp_load_fact_payment() INTO result_payments;
    
    RETURN TABLE(
        SELECT 'dim_customer' AS step, 'Completed' AS result, CURRENT_TIMESTAMP() AS completed_at
        UNION ALL SELECT 'dim_product', 'Completed', CURRENT_TIMESTAMP()
        UNION ALL SELECT 'fact_transaction_client_a', result_txn_a, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'fact_transaction_client_c', result_txn_c, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'fact_transaction_item', result_items, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'transaction_aggregates', result_agg, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'fact_order', result_orders, CURRENT_TIMESTAMP()
        UNION ALL SELECT 'fact_payment', result_payments, CURRENT_TIMESTAMP()
    );
END;
$$;

-- ============================================================================
-- EXECUTE LOAD (uncomment to run)
-- ============================================================================
-- CALL canonical.sp_load_all();

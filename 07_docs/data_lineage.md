# Data Lineage Documentation

## Overview

This document provides complete data lineage tracing from source files through all transformation layers to the final canonical model and data quality outputs.

## High-Level Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE FILES                                     │
├──────────────────┬──────────────────┬──────────────────┬────────────────────┤
│ ClientA XML (7)  │ ClientA TXT (1)  │ ClientC JSON (1) │ CSV Reference (8)  │
│ Transactions_1-7 │ Transactions_4   │ transactions     │ Customer, Orders,  │
│ .xml             │ .txt             │ .json            │ Products, Payments │
└────────┬─────────┴────────┬─────────┴────────┬─────────┴──────────┬─────────┘
         │                  │                  │                    │
         ▼                  ▼                  ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                               RAW LAYER                                       │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────┐ │
│  │ raw_xml_transactions│ │raw_txt_xml_transact.│ │ raw_json_transactions   │ │
│  │ VARIANT: raw_xml    │ │ VARCHAR: raw_text   │ │ VARIANT: raw_json       │ │
│  │ + audit columns     │ │ VARIANT: parsed_xml │ │ + audit columns         │ │
│  └─────────┬───────────┘ └─────────┬───────────┘ └───────────┬─────────────┘ │
│            │                       │                         │               │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────┐ │
│  │ raw_csv_customers   │ │ raw_csv_orders      │ │ raw_csv_products/pay.   │ │
│  │ VARCHAR columns     │ │ VARCHAR columns     │ │ VARCHAR columns         │ │
│  └─────────┬───────────┘ └─────────┬───────────┘ └───────────┬─────────────┘ │
└────────────┼─────────────────────────────────────────────────┼───────────────┘
             │                                                 │
             ▼                                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                             STAGING LAYER (Views)                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ v_stg_client_a_transactions          v_stg_client_c_transactions        │ │
│  │ • XMLGET parsing                     • JSON colon notation              │ │
│  │ • LATERAL FLATTEN for items          • COALESCE field variants          │ │
│  │ • Normalization (UPPER, TRIM)        • Name parsing from full_name      │ │
│  │ • Email validation regex             • DQ flags computed                │ │
│  │ • DQ flags computed                  │                                  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ v_stg_client_a_transaction_items     v_stg_client_c_transaction_items   │ │
│  │ • LATERAL FLATTEN on Items/Item      • LATERAL FLATTEN on items array   │ │
│  │ • Line total calculation             • Quantity/price normalization     │ │
│  │ • Metadata extraction (Warranty)     • Absolute values computed         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ v_stg_customers    v_stg_orders    v_stg_products    v_stg_payments     │ │
│  │ • Email validation • Date parsing  • SKU normalization • Amount parsing │ │
│  │ • Tier normalization • Status norm • Category cleanup  • Method norm    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────┬───────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           CANONICAL LAYER (Tables)                            │
│                                                                              │
│  DIMENSIONS:                                                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────────────┐       │
│  │dim_customer │ │ dim_product │ │  dim_date   │ │dim_payment_method│       │
│  │ SCD Type 2  │ │ SCD Type 2  │ │ Pre-loaded  │ │ Reference        │       │
│  │ customer_sk │ │ product_sk  │ │  date_sk    │ │payment_method_sk │       │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └────────┬─────────┘       │
│         │               │               │                 │                  │
│  FACTS: │               │               │                 │                  │
│  ┌──────▼───────────────▼───────────────▼─────────────────▼─────────────────┐│
│  │                      fact_transaction                                    ││
│  │  transaction_sk, transaction_id, order_id                                ││
│  │  customer_sk (FK), order_date_sk (FK), payment_method_sk (FK)           ││
│  │  payment_amount, payment_amount_abs, dq_status, dq_issues (VARIANT)     ││
│  └──────────────────────────────┬───────────────────────────────────────────┘│
│                                 │                                            │
│  ┌──────────────────────────────▼───────────────────────────────────────────┐│
│  │                    fact_transaction_item                                 ││
│  │  transaction_item_sk, transaction_sk (FK), product_sk (FK)              ││
│  │  line_number, sku, quantity, quantity_abs, unit_price, line_total       ││
│  │  item_metadata (VARIANT), dq_status, has_negative_quantity              ││
│  └──────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────┬───────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA QUALITY LAYER                                   │
│  ┌────────────────┐ ┌──────────────────┐ ┌─────────────────┐                │
│  │ dq_issue_log   │ │dq_duplicate_track│ │dq_summary_metric│                │
│  │ Issue details  │ │ Duplicate groups │ │ Batch metrics   │                │
│  └────────────────┘ └──────────────────┘ └─────────────────┘                │
│  ┌──────────────────────────────────────────────────────────┐               │
│  │                   REPORTING VIEWS                         │               │
│  │ v_dq_issue_summary, v_anomaly_detail_report,             │               │
│  │ v_file_quality_summary, v_reconciliation_summary         │               │
│  └──────────────────────────────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Detailed Field Lineage

### Transaction ID Lineage

```
SOURCE                      RAW                         STAGING                    CANONICAL
─────────────────────────────────────────────────────────────────────────────────────────────
XML: <TransactionID>   →   raw_xml_content         →   XMLGET(xml,              →   transaction_id
     TXN-1001              (VARIANT)                   'TransactionID'):"$"         (VARCHAR)
                                                       ::VARCHAR
                                                       UPPER(TRIM(...))

JSON: transaction_id   →   raw_json_content        →   COALESCE(                →   transaction_id
      or txn_id            (VARIANT)                   json:transaction_id,         (VARCHAR)
                                                       json:txn_id)
                                                       UPPER(TRIM(...))
```

### Customer Data Lineage

```
SOURCE                      RAW                         STAGING                    CANONICAL
─────────────────────────────────────────────────────────────────────────────────────────────
XML:                                                                              
<Customer>             →   raw_xml_content         →   customer_id =            →   dim_customer
  <CustomerID>             (VARIANT)                   XMLGET(Customer,             customer_sk
  <Name>                                               'CustomerID')                customer_id
    <FirstName>                                        first_name =                 first_name
    <LastName>                                         XMLGET(Name,'FirstName')     last_name
  <Email>                                              email =                      email
  <LoyaltyTier>*                                       LOWER(TRIM(Email))           loyalty_tier
</Customer>                                            email_validation_status      email_validation_status

JSON:                                                                             
customer: {            →   raw_json_content        →   customer_id =            →   dim_customer
  customer_id/id,          (VARIANT)                   COALESCE(customer_id,id)     (merged)
  first_name/name,                                     first_name =                 
  last_name,                                           CASE WHEN first_name        
  email,                                                 IS NULL THEN               
  loyalty_tier/tier                                      SPLIT_PART(name,' ',1)    
}                                                      ...                          

CSV:                   →   raw_csv_customers       →   v_stg_customers          →   dim_customer
customer_id,               customer_id_raw             customer_id =                (merged/
first_name,                first_name_raw              UPPER(TRIM(...))             priority)
last_name,                 ...                         first_name =                 
email,                     (VARCHAR columns)           INITCAP(TRIM(...))           
loyalty_tier                                           ...                          
```

### Payment Amount Lineage

```
SOURCE                      RAW                         STAGING                    CANONICAL
─────────────────────────────────────────────────────────────────────────────────────────────
XML:                   →   raw_xml_content         →   payment_amount =         →   payment_amount
<Payment>                  (VARIANT)                   TRY_TO_DECIMAL(              payment_amount_abs
  <Amount                                              XMLGET(Amount):"$",          (original + ABS)
    currency="USD">                                    18, 4)                       
    97.48                                              payment_amount_abs =         
  </Amount>                                            ABS(payment_amount)          
</Payment>                                             is_negative_payment =        
                                                       payment_amount < 0           

JSON:                  →   raw_json_content        →   payment_amount =         →   payment_amount
payment: {                 (VARIANT)                   TRY_TO_DECIMAL(              payment_amount_abs
  amount/total,                                        COALESCE(amount,total),      
  currency                                             18, 4)                       
}                                                      ...                          
```

## Transformation Rules Summary

### Normalization Rules

| Field | Rule | Example |
|-------|------|---------|
| customer_id | UPPER(TRIM(value)) | " cust-001 " → "CUST-001" |
| transaction_id | UPPER(TRIM(value)) | "txn-1001" → "TXN-1001" |
| email | LOWER(TRIM(value)) | " John@Email.COM " → "john@email.com" |
| first_name | INITCAP(TRIM(value)) | "JOHN" → "John" |
| payment_method | Mapped to standard codes | "CreditCard" → "CREDIT_CARD" |
| currency | UPPER with USD default | NULL → "USD" |

### Data Quality Rules Applied

| Stage | Rule | Flag Generated |
|-------|------|----------------|
| Staging | NULL or empty check | is_missing_* |
| Staging | Negative value check | is_negative_* |
| Staging | Email regex validation | email_validation_status |
| Staging | Date parsing | is_invalid_order_date |
| Canonical | DQ status classification | dq_status (VALID/WARNING/INVALID/CRITICAL) |
| DQ Layer | Duplicate detection | dq_duplicate_tracker |

## Audit Trail Columns

Every record carries lineage metadata:

| Column | Description | Example |
|--------|-------------|---------|
| raw_record_id | Unique ID in raw layer | 12345 |
| source_file_name | Original file name | ClientA_Transactions_1.xml |
| source_client | Client identifier | ClientA, ClientC |
| source_format | File format | XML, JSON, CSV |
| batch_id | Ingestion batch | BATCH_20251115_143022 |
| ingestion_timestamp | When loaded to raw | 2025-11-15 14:30:22 |
| record_hash | SHA256 of source content | a1b2c3d4... |

## Reconciliation Points

```sql
-- Record counts should flow through:
SELECT 'RAW_XML' AS layer, COUNT(*) FROM raw.raw_xml_transactions
UNION ALL
SELECT 'STAGING_A', COUNT(*) FROM staging.v_stg_client_a_transactions
UNION ALL  
SELECT 'CANONICAL', COUNT(*) FROM canonical.fact_transaction WHERE source_client = 'ClientA'
UNION ALL
SELECT 'DQ_ISSUES', COUNT(*) FROM data_quality.dq_issue_log WHERE source_client = 'ClientA';
```

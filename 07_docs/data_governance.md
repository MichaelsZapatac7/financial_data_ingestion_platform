# Data Governance Framework

## Overview

This document describes how the Financial Data Ingestion Platform implements data governance principles to ensure auditability, traceability, reproducibility, and comprehensive anomaly tracking.

## Governance Pillars

### 1. Auditability

**Definition**: The ability to trace any data value back to its source and understand all transformations applied.

#### Implementation

```
┌─────────────────────────────────────────────────────────────────┐
│                    AUDIT TRAIL COMPONENTS                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │ Source File │ →  │  Raw Layer  │ →  │  Audit Columns      │ │
│  │   Metadata  │    │  VARIANT    │    │  - raw_record_id    │ │
│  │             │    │  storage    │    │  - source_file_name │ │
│  └─────────────┘    └─────────────┘    │  - batch_id         │ │
│                                        │  - ingestion_ts     │ │
│                                        │  - record_hash      │ │
│                                        └─────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              INGESTION AUDIT LOG                         │   │
│  │  - Batch start/end times                                │   │
│  │  - Rows loaded/rejected per file                        │   │
│  │  - Error messages and details                           │   │
│  │  - User who executed ingestion                          │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

#### Key Audit Columns

| Column | Location | Purpose |
|--------|----------|---------|
| `raw_record_id` | All raw tables | Unique identifier for source record |
| `source_file_name` | All layers | Track origin file |
| `batch_id` | All layers | Group records by ingestion run |
| `ingestion_timestamp` | All layers | When data was loaded |
| `record_hash` | Raw layer | Detect changes/duplicates |
| `created_at` | Canonical | When canonical record created |
| `created_by` | Canonical | User/process that created |

#### Audit Query Examples

```sql
-- Trace a transaction back to source
SELECT 
    ft.transaction_id,
    ft.source_file_name,
    ft.batch_id,
    ft.ingestion_timestamp,
    ft.raw_record_id,
    rxt.raw_xml_content  -- Original source data
FROM canonical.fact_transaction ft
JOIN raw.raw_xml_transactions rxt ON ft.raw_record_id = rxt.raw_record_id
WHERE ft.transaction_id = 'TXN-1001';

-- Audit log for batch
SELECT * FROM raw.ingestion_audit_log 
WHERE batch_id = 'BATCH_20251115_143022';
```

---

### 2. Traceability

**Definition**: The ability to follow data through all transformation stages and understand relationships.

#### Implementation

```
SOURCE FILE                RAW                 STAGING              CANONICAL
───────────────────────────────────────────────────────────────────────────────
ClientA_Trans_1.xml   →   raw_record_id=1  →  v_stg view    →   transaction_sk=101
                          source_file_name     raw_record_id      raw_record_id=1
                          record_hash          source_file_name   source_file_name
                                               batch_id           batch_id
                                                                  dq_issues (JSON)
```

#### Traceability Features

1. **Forward Traceability** (Source → Target)
   - `raw_record_id` flows from raw to canonical
   - `source_file_name` preserved at all layers
   - `batch_id` links all records from same ingestion

2. **Backward Traceability** (Target → Source)
   - Every canonical record has `raw_record_id`
   - Join back to raw tables for original content
   - `record_hash` validates source integrity

3. **Lateral Traceability** (Related Records)
   - `transaction_sk` links header to line items
   - `customer_sk` links transactions to customers
   - `product_sk` links items to products

#### Traceability Query Examples

```sql
-- Forward: Find all canonical records from a source file
SELECT ft.*, fi.* 
FROM canonical.fact_transaction ft
LEFT JOIN canonical.fact_transaction_item fi ON ft.transaction_sk = fi.transaction_sk
WHERE ft.source_file_name = 'ClientA_Transactions_1.xml';

-- Backward: Get original XML for a canonical transaction
SELECT 
    ft.transaction_id,
    rxt.raw_xml_content AS original_xml
FROM canonical.fact_transaction ft
JOIN raw.raw_xml_transactions rxt ON ft.raw_record_id = rxt.raw_record_id
WHERE ft.transaction_sk = 101;
```

---

### 3. Reproducibility

**Definition**: The ability to recreate any data state given the same inputs and transformation logic.

#### Implementation

```
┌─────────────────────────────────────────────────────────────────┐
│                  REPRODUCIBILITY COMPONENTS                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. SOURCE PRESERVATION                                         │
│     ┌─────────────────────────────────────────────────────┐    │
│     │ Raw Layer stores COMPLETE source content            │    │
│     │ • XML: Full document in VARIANT                     │    │
│     │ • JSON: Full document in VARIANT                    │    │
│     │ • CSV: All columns as VARCHAR (no coercion)         │    │
│     │ • No data modification at raw layer                 │    │
│     └─────────────────────────────────────────────────────┘    │
│                                                                 │
│  2. TRANSFORMATION AS CODE                                      │
│     ┌─────────────────────────────────────────────────────┐    │
│     │ • All transformations in SQL views/procedures       │    │
│     │ • Version controlled in Git                         │    │
│     │ • No external tool dependencies                     │    │
│     │ • Deterministic logic (no random, timestamps ok)    │    │
│     └─────────────────────────────────────────────────────┘    │
│                                                                 │
│  3. BATCH ISOLATION                                             │
│     ┌─────────────────────────────────────────────────────┐    │
│     │ • Each ingestion has unique batch_id                │    │
│     │ • Can replay specific batch                         │    │
│     │ • Time travel enabled (7-day retention)             │    │
│     └─────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Reproducibility Query Examples

```sql
-- Replay a batch (conceptual)
-- 1. Delete canonical records for batch
DELETE FROM canonical.fact_transaction WHERE batch_id = 'BATCH_XXX';
DELETE FROM canonical.fact_transaction_item WHERE batch_id = 'BATCH_XXX';

-- 2. Re-run load procedures
CALL canonical.sp_load_all();

-- Time travel query (see data as it was)
SELECT * FROM canonical.fact_transaction 
AT(TIMESTAMP => '2025-11-15 10:00:00'::TIMESTAMP);
```

---

### 4. Anomaly Tracking

**Definition**: Comprehensive detection, logging, and reporting of all data quality issues.

#### Implementation

```
┌─────────────────────────────────────────────────────────────────┐
│                   ANOMALY TRACKING FRAMEWORK                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DETECTION LAYER (Staging Views)                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • is_missing_transaction_id  • is_negative_quantity     │   │
│  │ • is_missing_customer_id     • is_negative_unit_price   │   │
│  │ • is_missing_order_id        • is_negative_payment      │   │
│  │ • is_missing_email           • email_validation_status  │   │
│  │ • is_invalid_order_date      • has_empty_items          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                     │
│                           ▼                                     │
│  CLASSIFICATION (Canonical Load)                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dq_status:                                              │   │
│  │   CRITICAL - Missing transaction ID                     │   │
│  │   INVALID  - Missing customer/order ID, negative values │   │
│  │   WARNING  - Missing email/date, format issues          │   │
│  │   VALID    - No issues detected                         │   │
│  │                                                         │   │
│  │ dq_issues: VARIANT containing all flags as JSON         │   │
│  │ dq_issue_count: Number of issues on record              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                     │
│                           ▼                                     │
│  LOGGING (DQ Layer)                                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dq_issue_log:                                           │   │
│  │   • issue_type, issue_category, issue_severity          │   │
│  │   • record_identifier, field_name, field_value_raw      │   │
│  │   • source_file_name, batch_id                          │   │
│  │   • resolution_action, is_resolved                      │   │
│  │                                                         │   │
│  │ dq_duplicate_tracker:                                   │   │
│  │   • duplicate_key, occurrence_count                     │   │
│  │   • all_occurrence_files (ARRAY)                        │   │
│  │   • resolution_method                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                     │
│                           ▼                                     │
│  REPORTING (DQ Views)                                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ v_dq_issue_summary      - Aggregated by type/severity   │   │
│  │ v_anomaly_detail_report - Transaction-level details     │   │
│  │ v_file_quality_summary  - Quality per source file       │   │
│  │ v_batch_quality_summary - Overall batch metrics         │   │
│  │ v_reconciliation_summary - Record counts across layers  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

#### Anomaly Categories

| Category | Examples | Severity |
|----------|----------|----------|
| COMPLETENESS | Missing required fields | CRITICAL to MEDIUM |
| VALIDITY | Negative values, out of range | HIGH |
| FORMAT | Invalid email, unparseable date | LOW to MEDIUM |
| UNIQUENESS | Duplicate transactions | CRITICAL |
| CONSISTENCY | Payment ≠ line total | WARNING |

---

## Governance Controls Summary

| Control | Implementation | Verification |
|---------|----------------|--------------|
| **Data Retention** | 7-90 days per schema | Time travel queries |
| **Change Tracking** | CHANGE_TRACKING = TRUE | CHANGES clause |
| **Access Control** | Schema-level grants | SHOW GRANTS |
| **Encryption** | Snowflake default | Platform managed |
| **Lineage** | Audit columns + raw preservation | JOIN queries |
| **Quality** | DQ flags + issue logging | DQ views |

## Compliance Considerations

- **GDPR**: Email stored, can be masked/deleted via customer_sk
- **SOX**: Full audit trail for financial transactions
- **Data Retention**: Configurable per schema (7-90 days)
- **Right to Erasure**: Delete by customer_id cascades via SK

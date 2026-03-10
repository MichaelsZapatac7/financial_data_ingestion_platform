# Architecture Documentation

## Overview

The Financial Data Ingestion Platform implements a **layered data architecture** using Snowflake SQL-only approach.

## Architectural Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCE FILES                               │
│   XML (ClientA)  │  JSON (ClientC)  │  CSV (Reference)  │ TXT  │
└────────┬─────────┴────────┬─────────┴────────┬──────────┴──┬───┘
         │                  │                  │             │
         ▼                  ▼                  ▼             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        RAW LAYER                                │
│  • VARIANT columns for XML/JSON                                 │
│  • VARCHAR columns for CSV                                      │
│  • Full source fidelity preserved                               │
│  • Audit columns: source_file, batch_id, ingestion_timestamp    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER                              │
│  • Views for parsing (XMLGET, JSON colon notation)              │
│  • LATERAL FLATTEN for nested structures                        │
│  • Field normalization (casing, formatting)                     │
│  • Data quality flags computed                                  │
│  • Metadata extraction for unexpected fields                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     CANONICAL LAYER                             │
│  • Kimball star schema design                                   │
│  • Dimensions: dim_customer, dim_product, dim_date              │
│  • Facts: fact_transaction, fact_transaction_item               │
│  • SCD Type 2 for slowly changing dimensions                    │
│  • Surrogate keys via sequences                                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DATA QUALITY LAYER                            │
│  • dq_issue_log: Detailed issue tracking                        │
│  • dq_duplicate_tracker: Duplicate detection                    │
│  • dq_rejected_records: Failed validations                      │
│  • dq_summary_metrics: Batch-level metrics                      │
│  • Reporting views for monitoring                               │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. VARIANT for Semi-Structured Data
- XML and JSON stored as VARIANT preserves all original data
- No data loss even with schema evolution
- Enables flexible querying with XMLGET and JSON path notation

### 2. Views for Staging
- No ETL tool dependency
- Logic is version-controlled SQL
- Easy to modify and test
- Computed at query time (or materialized as needed)

### 3. Stored Procedures for Loading
- Encapsulates business logic
- Supports incremental loading
- Returns execution metrics
- Easy to orchestrate

### 4. SCD Type 2 for Dimensions
- Tracks historical changes
- effective_from / effective_to dates
- is_current flag for current record
- version_number for tracking

### 5. Data Quality Integration
- DQ flags computed in staging
- DQ issues logged automatically
- Metrics calculated per batch
- Reporting views for monitoring

## File Processing Strategy

| Format | Parsing Method | Key Functions |
|--------|----------------|---------------|
| XML | XMLGET, LATERAL FLATTEN | Extract nested elements |
| JSON | Colon notation, LATERAL FLATTEN | Handle multiple field names |
| CSV | Direct column mapping | VARCHAR preservation |
| TXT | Parse as CSV, then PARSE_XML | Two-stage processing |

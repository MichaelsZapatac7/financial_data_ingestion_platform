# Operations Runbook

## Daily Operations

### Morning Health Check
```sql
-- Check last ingestion status
SELECT * FROM raw.ingestion_audit_log 
ORDER BY created_at DESC LIMIT 10;

-- Check DQ metrics trend
SELECT * FROM data_quality.v_batch_quality_summary
WHERE metric_date >= CURRENT_DATE - 7
ORDER BY metric_date DESC;

-- Check for critical issues
SELECT COUNT(*) AS critical_issues
FROM data_quality.dq_issue_log
WHERE issue_severity = 'CRITICAL'
  AND is_resolved = FALSE;
```

### Troubleshooting Common Issues

#### 1. COPY INTO Failures
```sql
-- Check copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_XML_TRANSACTIONS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));

-- Validate staged files
LIST @raw.stg_xml_files;
```

#### 2. High Rejection Rate
```sql
-- Identify problematic files
SELECT source_file_name, dq_status, COUNT(*)
FROM canonical.fact_transaction
WHERE batch_id = 'BATCH_XXX'
GROUP BY source_file_name, dq_status
ORDER BY COUNT(*) DESC;
```

#### 3. Missing Data
```sql
-- Reconciliation check
SELECT * FROM data_quality.v_reconciliation_summary;
```

## Emergency Procedures

### Rollback a Batch
```sql
-- 1. Identify records to remove
SELECT COUNT(*) FROM canonical.fact_transaction WHERE batch_id = 'BATCH_XXX';

-- 2. Delete in reverse order
DELETE FROM canonical.fact_transaction_item 
WHERE transaction_sk IN (SELECT transaction_sk FROM canonical.fact_transaction WHERE batch_id = 'BATCH_XXX');

DELETE FROM canonical.fact_transaction WHERE batch_id = 'BATCH_XXX';

-- 3. Clear DQ logs
DELETE FROM data_quality.dq_issue_log WHERE batch_id = 'BATCH_XXX';
```

### Reprocess a File
```sql
-- 1. Remove old data
DELETE FROM raw.raw_xml_transactions WHERE source_file_name = 'ClientA_Transactions_1.xml';

-- 2. Re-upload and re-ingest
-- PUT file:///path/to/file @raw.stg_xml_files OVERWRITE=TRUE;
-- Run COPY INTO again
```

## Monitoring Queries

### Performance Metrics
```sql
-- Query execution times
SELECT query_id, query_text, total_elapsed_time/1000 AS seconds
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE warehouse_name = 'FINANCIAL_ETL_WH'
  AND start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

### Storage Usage
```sql
SELECT 
    TABLE_SCHEMA,
    SUM(BYTES)/1024/1024 AS size_mb,
    SUM(ROW_COUNT) AS total_rows
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'FINANCIAL_DATA_PLATFORM'
GROUP BY TABLE_SCHEMA;
```

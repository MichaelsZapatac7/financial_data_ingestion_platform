# Source File Inventory

## Overview

This document catalogs all source files for the financial data ingestion platform.

## Transactional Files

| File Name | Format | Client | Notes |
|-----------|--------|--------|-------|
| ClientA_Transactions_1.xml | XML | ClientA | Contains duplicate TXN-1001, negative quantities, extra fields |
| ClientA_Transactions_2.xml | XML | ClientA | Duplicate TXN-1009, missing fields, negative amounts |
| ClientA_Transactions_3.xml | XML | ClientA | Missing names, payment methods, transaction IDs |
| ClientA_Transactions_4.txt | TXT (XML content) | ClientA | XML embedded in TXT, needs special parsing |
| ClientA_Transactions_5.xml | XML | ClientA | Missing customer IDs, order IDs, payment amounts |
| ClientA_Transactions_6.xml | XML | ClientA | Expected (if exists) |
| ClientA_Transactions_7.xml | XML | ClientA | Expected (if exists) |
| transactions.json | JSON | ClientC | JSON array with snake_case fields |

## Reference Data Files

| File Name | Format | Entity | Notes |
|-----------|--------|--------|-------|
| Customer.csv | CSV | Customers | Lowercase extension |
| Customer.CSV | CSV | Customers | Uppercase extension - may be duplicate |
| Orders.csv | CSV | Orders | Plural naming |
| Order.csv | CSV | Orders | Singular naming variant |
| Products.csv | CSV | Products | Plural naming |
| Product.csv | CSV | Products | Singular naming variant |
| Payments.csv | CSV | Payments | Payment records |

## Known Data Quality Issues

### Structural Issues
- Duplicate transaction IDs across files
- Duplicate order IDs
- Missing required fields (IDs, dates, emails)
- Inconsistent field naming (snake_case vs camelCase)

### Value Issues
- Negative quantities
- Negative prices
- Negative payment amounts
- Invalid email formats
- Empty string values

### Unexpected Nested Structures
- `<LoyaltyTier>`, `<Metadata>`, `<Tags>`, `<Notes>`
- `<Preferences>`, `<Flags>`, `<Attributes>`
- `<Warranty>`, `<GiftOptions>`
- `<Fees>` in payment section

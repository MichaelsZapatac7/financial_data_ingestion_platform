# Source File Inventory

## Overview

This document catalogs all source files for the financial data ingestion platform.

## Transactional Files

| File Name | Format | Client | Records | Notes |
|-----------|--------|--------|---------|-------|
| ClientA_Transactions_1.xml | XML | ClientA | 5 | Duplicate TXN-1001, negative quantities, LoyaltyTier field |
| ClientA_Transactions_2.xml | XML | ClientA | 6 | Duplicate TXN-1009, missing fields, Preferences/Flags |
| ClientA_Transactions_3.xml | XML | ClientA | 6 | Missing names, payment methods, Attributes field |
| ClientA_Transactions_4.txt | TXT (XML) | ClientA | 7 | XML in TXT format, Notes/Tags/Warranty fields |
| ClientA_Transactions_5.xml | XML | ClientA | 8 | Missing customer/order IDs, GiftOptions/Preferences |
| ClientA_Transactions_6.xml | XML | ClientA | TBD | Additional transactions |
| ClientA_Transactions_7.xml | XML | ClientA | TBD | Additional transactions |
| transactions.json | JSON | ClientC | TBD | Snake_case fields, array structure |

## Reference Data Files

| File Name | Format | Entity | Expected Columns |
|-----------|--------|--------|------------------|
| Customer.csv | CSV | Customers | customer_id, first_name, last_name, email, loyalty_tier, signup_source, is_active |
| Customer.CSV | CSV | Customers | Same as above (case variant) |
| Orders.csv | CSV | Orders | order_id, customer_id, order_date, status, total_amount, currency |
| Order.csv | CSV | Orders | Same as above (naming variant) |
| Products.csv | CSV | Products | product_id, sku, product_name, description, category, price, currency |
| Product.csv | CSV | Products | Same as above (naming variant) |
| Payments.csv | CSV | Payments | payment_id, order_id, transaction_id, payment_method, amount, currency, payment_date, status |

## Known Data Quality Issues

### Missing Fields
- Empty TransactionID (TXN-1002, TXN-1014)
- Empty CustomerID (TXN-1022)
- Empty OrderID (TXN-1028)
- Empty Email (TXN-1004, TXN-1016)
- Empty OrderDate (TXN-1002, TXN-1008, TXN-1019)
- Empty PaymentMethod (TXN-1012)
- Empty PaymentAmount (TXN-1026)
- Empty SKU (TXN-1002, TXN-1006, TXN-1017)
- Empty Description (TXN-1024)
- Empty Names (TXN-1010)

### Invalid Values
- Negative quantities: TXN-1001, TXN-1003, TXN-1008, TXN-1015, TXN-1019, TXN-1027
- Negative prices: TXN-1011, TXN-1023
- Negative payment amounts: TXN-1005, TXN-1011, TXN-1017, TXN-1023
- Invalid email format: TXN-1007 (invalid-email)
- Zero prices: TXN-1002

### Duplicates
- TXN-1001: Appears twice in ClientA_Transactions_1.xml
- TXN-1009: Appears twice in ClientA_Transactions_2.xml
- TXN-1021: Appears twice in ClientA_Transactions_4.txt
- ORD-5013: Used by both TXN-1013 and TXN-1015
- CUST-A-0001: Appears in multiple transactions (valid repeat customer)

### Unexpected Nested Structures
| Structure | Location | Transactions |
|-----------|----------|--------------|
| LoyaltyTier | Customer | TXN-1001 |
| Metadata | Customer | TXN-1003 |
| Tags | Customer | TXN-1004, TXN-1018 |
| Preferences | Customer | TXN-1006, TXN-1022 |
| Flags | Customer | TXN-1007 |
| Attributes | Customer/Item | TXN-1010, TXN-1013 |
| Notes | Customer | TXN-1016 |
| Warranty | Item | TXN-1020 |
| GiftOptions | Item | TXN-1025 |
| Fees | Payment | TXN-1003 |

## File Processing Order

1. CSV Reference Files (establish master data)
2. XML Transaction Files (bulk of transactions)
3. TXT-XML File (special parsing required)
4. JSON Transaction File (alternative format)

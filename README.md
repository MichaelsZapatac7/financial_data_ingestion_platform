# Financial Data Ingestion Platform

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Executive Summary

Production-ready **Snowflake SQL-only** solution for ingesting multi-format financial transaction data from multiple clients. The platform handles XML, JSON, and CSV files with comprehensive data quality controls, anomaly detection, and full audit trail capabilities.

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Architecture](#architecture)
3. [Ingestion Flow](#ingestion-flow)
4. [Canonical Model](#canonical-model)
5. [Anomaly Handling](#anomaly-handling)
6. [Data Quality Controls](#data-quality-controls)
7. [CI/CD Approach](#cicd-approach)
8. [Quick Start](#quick-start)
9. [Project Structure](#project-structure)
10. [Assumptions](#assumptions)

---

## Problem Statement

### Business Context

A financial services organization receives transaction data from multiple clients in varying formats:

- **ClientA**: XML transaction files with nested structures
- **ClientC**: JSON transaction files with field name variations
- **Reference Data**: CSV master files (customers, products, orders, payments)

### Technical Challenges

| Challenge | Description |
|-----------|-------------|
| **Format Diversity** | XML, JSON, CSV, TXT (containing XML) |
| **Schema Variations** | Different field names, nesting depths, optional fields |
| **Data Quality Issues** | Duplicates, missing fields, invalid formats, negative values |
| **Unexpected Structures** | Metadata, tags, preferences appearing without notice |
| **Audit Requirements** | Complete traceability from source to analytics |

### Solution Requirements

1. ✅ Ingest all file formats using **SQL only** (no external tools)
2. ✅ Design a **unified canonical model** for analytics
3. ✅ Transform raw data with **full normalization**
4. ✅ Detect and handle **all anomalies** without data loss
5. ✅ Provide complete **audit trail** and **data lineage**

---

## Architecture

### Layered Data Architecture


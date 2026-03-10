/*
================================================================================
FILE FORMATS - COMPLETE CONFIGURATION
================================================================================
*/

USE DATABASE financial_data_platform;
USE SCHEMA raw;

-- XML FILE FORMAT
CREATE OR REPLACE FILE FORMAT raw.ff_xml
    TYPE = 'XML'
    COMPRESSION = 'AUTO'
    PRESERVE_SPACE = FALSE
    STRIP_OUTER_ELEMENT = FALSE
    DISABLE_SNOWFLAKE_DATA = FALSE
    DISABLE_AUTO_CONVERT = FALSE
    IGNORE_UTF8_ERRORS = TRUE
    COMMENT = 'XML format for ClientA transaction files';

-- JSON FILE FORMAT - Standard
CREATE OR REPLACE FILE FORMAT raw.ff_json
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    DATE_FORMAT = 'AUTO'
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    BINARY_FORMAT = 'HEX'
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL', 'null', 'None', 'none', 'NONE')
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = TRUE
    COMMENT = 'JSON format for ClientC transaction files';

-- JSON FILE FORMAT - With Outer Array Stripped
CREATE OR REPLACE FILE FORMAT raw.ff_json_array
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE
    ALLOW_DUPLICATE = TRUE
    IGNORE_UTF8_ERRORS = TRUE
    COMMENT = 'JSON format with outer array stripped';

-- CSV FILE FORMAT - Standard (with header)
CREATE OR REPLACE FILE FORMAT raw.ff_csv_standard
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\\'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('', 'NULL', 'null', 'N/A', 'n/a', '-', 'NA', 'na', '#N/A')
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'Standard CSV with header row';

-- CSV FILE FORMAT - No Header
CREATE OR REPLACE FILE FORMAT raw.ff_csv_no_header
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('', 'NULL', 'null')
    COMMENT = 'CSV without header row';

-- CSV FILE FORMAT - Pipe Delimited
CREATE OR REPLACE FILE FORMAT raw.ff_csv_pipe
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL', 'null')
    COMMENT = 'Pipe-delimited CSV with header';

-- TXT FILE FORMAT - Raw Text
CREATE OR REPLACE FILE FORMAT raw.ff_txt_raw
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = 'NONE'
    RECORD_DELIMITER = 'NONE'
    TRIM_SPACE = FALSE
    COMMENT = 'Raw text format - loads entire file as single field';

-- TXT FILE FORMAT - Line by Line
CREATE OR REPLACE FILE FORMAT raw.ff_txt_lines
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = 'NONE'
    RECORD_DELIMITER = '\n'
    TRIM_SPACE = FALSE
    COMMENT = 'Text format - one record per line';

-- Verification
SHOW FILE FORMATS IN SCHEMA raw;

SELECT
    'File formats created successfully' AS status,
    COUNT(*) AS format_count
FROM information_schema.file_formats
WHERE file_format_schema = 'RAW';

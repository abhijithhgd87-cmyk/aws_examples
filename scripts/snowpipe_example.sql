-- Snowpipe example SQL snippets
-- 1) Create file format and external stage (AWS S3 example)
CREATE OR REPLACE FILE FORMAT etl_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;

CREATE OR REPLACE STAGE ext_stage
  URL='s3://your-bucket/path'
  FILE_FORMAT = etl_csv_format
  STORAGE_INTEGRATION = your_storage_integration;

-- 2) Create staging table
CREATE OR REPLACE TABLE stg_table (data VARIANT, ingest_ts TIMESTAMP_LTZ, source_file STRING, record_hash STRING);

-- 3) Create a pipe for continuous ingestion (requires cloud notifications integration)
CREATE OR REPLACE PIPE etl_pipe
  AUTO_INGEST = true
  AS
  COPY INTO stg_table (data, ingest_ts, source_file, record_hash)
  FROM (SELECT parse_json($1), current_timestamp(), metadata$filename, null FROM @ext_stage)
  FILE_FORMAT = (FORMAT_NAME = etl_csv_format);

-- Notes:
-- - To use AUTO_INGEST, configure a storage integration and set up notifications (SNS/SQS for AWS).
-- - Alternatively, use the Snowpipe REST API to notify and load files programmatically.

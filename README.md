Project: Snowflake ETL for CSV/JSON ingestion

Structure

- data/
  - raw/csv/  # place input CSV files here
  - raw/json/ # place input JSON files here
- scripts/    # ingestion and utility scripts
- logs/       # log output
- tests/      # minimal unit/integration tests

Overview

This project demonstrates a small ETL pipeline that:
1. Picks up CSV and JSON files from `data/raw`.
2. Uploads them to a Snowflake user stage.
3. Loads them into a staging table.
4. Applies audit columns (ingest_ts, source_file, record_hash).
5. Merges into a BI gold table.

Requirements

- Python 3.8+
- Install dependencies from `requirements.txt`.
- A Snowflake account with a database, schema and warehouse.

Security

- Use environment variables for Snowflake credentials (see `scripts/config_example.env`).
- Do NOT commit real credentials.

Next steps

- Update the environment variables with your Snowflake connection details.
- Place sample files into `data/raw/csv` and `data/raw/json`.
- Run the ingestion script: `python scripts/ingest.py`
## Snowpipe (continuous ingestion) use case

When you need near-real-time ingestion from cloud storage (S3, GCS, Azure), Snowpipe is the managed service to load files automatically into Snowflake.

Common approaches:
- Cloud storage event notifications -> Snowpipe (auto-ingest). Files landing in the external stage trigger a COPY INTO automatically.
- Snowpipe REST API -> push file metadata to Snowpipe programmatically when you cannot use cloud notifications.

Quick example (external stage + pipe using S3 notifications):

1. Create an external stage and file format:

```sql
CREATE OR REPLACE FILE FORMAT etl_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;
CREATE OR REPLACE STAGE ext_stage URL='s3://your-bucket/path'
  FILE_FORMAT = etl_csv_format
  STORAGE_INTEGRATION = your_storage_integration;
```

2. Create a pipe that copies from the stage into a staging table:

```sql
CREATE OR REPLACE TABLE stg_table (data VARIANT, ingest_ts TIMESTAMP_LTZ, source_file STRING, record_hash STRING);

CREATE OR REPLACE PIPE etl_pipe
  AUTO_INGEST = true
  AS
  COPY INTO stg_table (data, ingest_ts, source_file, record_hash)
  FROM (SELECT parse_json($1), current_timestamp(), metadata$filename, null FROM @ext_stage)
  FILE_FORMAT = (FORMAT_NAME = etl_csv_format);
```

3. Configure an S3 notification integration in Snowflake and in your AWS bucket so S3 events notify Snowflake. After that, files dropped into the bucket are ingested automatically.

If you cannot use cloud notifications, use the Snowpipe REST API to notify Snowflake about files you placed in an internal or external stage. See `scripts/snowpipe_example.sql` for the SQL portion and Snowflake docs for REST examples.

## Time Travel use case

Snowflake Time Travel lets you query, clone, and restore data from a previous point in time.
Use cases: accidental deletes or updates, auditing, and point-in-time clones for debugging.

Examples:

1. Query a table as-of a past timestamp:

```sql
SELECT * FROM my_schema.my_table AT (TIMESTAMP => '2026-02-19 12:00:00'::timestamp_ltz);
```

2. Create a point-in-time clone of a table (fast, zero-copy):

```sql
CREATE TABLE my_table_clone CLONE my_schema.my_table AT (TIMESTAMP => '2026-02-19 12:00:00'::timestamp_ltz);
```

3. Undrop a recently dropped table (if within retention):

```sql
UNDROP TABLE my_schema.my_table;
```

Notes:
- Time Travel retention depends on your Snowflake edition and table/database-level settings.
- Cloning is useful for creating a safe snapshot for BI gold refreshes or diagnostics without extra storage cost (until changes are made).

## Where to go next
- Fill `scripts/config.env` with your Snowflake credentials.
- For production Snowpipe you must configure a cloud storage integration and notification channel (AWS SQS/SNS, GCS Pub/Sub, Azure Event Grid).
- See `scripts/snowpipe_example.sql` and `scripts/time_travel_example.md` for concise examples you can adapt.


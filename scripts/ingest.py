"""Ingest CSV and JSON files into Snowflake staging, apply audit columns and merge to gold table.

Usage: fill a `.env` file from `config_example.env`, then run:
  python scripts/ingest.py

This script is a minimal runnable example. For production use, add error handling, retries,
parallelization, backoff, schema discovery and schema evolution handling.
"""
import os
import glob
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector

load_dotenv(dotenv_path=Path(__file__).parent / 'config.env')

LOG = logging.getLogger('ingest')
LOG.setLevel(logging.INFO)
fh = logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'ingest.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
LOG.addHandler(fh)

SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PWD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SF_ROLE = os.getenv('SNOWFLAKE_ROLE')

DATA_DIR = Path(__file__).parent.parent / 'data' / 'raw'
CSV_DIR = DATA_DIR / 'csv'
JSON_DIR = DATA_DIR / 'json'

STAGE_NAME = 'etl_stage'
STG_TABLE = 'stg_table'
GOLD_TABLE = 'gold_table'


def get_conn():
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PWD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE,
    )


def record_hash(row: pd.Series) -> str:
    m = hashlib.sha256()
    # create deterministic string from row values
    concat = '|'.join([str(x) for x in row.values.tolist()])
    m.update(concat.encode('utf-8'))
    return m.hexdigest()


def upload_file_to_stage(conn, local_path: Path, stage_name: str):
    cursor = conn.cursor()
    try:
        sql = f"PUT file://{local_path.as_posix()} @{stage_name} OVERWRITE = TRUE"
        LOG.info('PUT %s', local_path)
        cursor.execute(sql)
    finally:
        cursor.close()


def load_csv_to_staging(conn, local_path: Path):
    df = pd.read_csv(local_path)
    df['ingest_ts'] = datetime.utcnow().isoformat()
    df['source_file'] = local_path.name
    df['record_hash'] = df.apply(record_hash, axis=1)
    # write temp parquet file and use Snowflake PUT + COPY INTO for production; here we'll use INSERT for demo
    cols = ','.join([f'"{c}"' for c in df.columns])
    values = ','.join([f"'{v}'".replace("nan'", "''") for v in df.iloc[0].values]) if len(df)>0 else ''
    # For demo, create table if not exists and insert rows
    cur = conn.cursor()
    try:
        # create table with variant schema (simple VARCHARs) for demo; adapt types in production
        cur.execute(f"CREATE TABLE IF NOT EXISTS {STG_TABLE} (data VARIANT, ingest_ts TIMESTAMP_LTZ, source_file STRING, record_hash STRING)")
        for _, row in df.iterrows():
            row_json = row.drop(['ingest_ts','source_file','record_hash']).to_json(orient='records')
            cur.execute(
                f"INSERT INTO {STG_TABLE}(data, ingest_ts, source_file, record_hash) VALUES (parse_json(%s), to_timestamp_ltz(%s), %s, %s)",
                (row_json, row['ingest_ts'], row['source_file'], row['record_hash'])
            )
        conn.commit()
    finally:
        cur.close()


def load_json_to_staging(conn, local_path: Path):
    # read JSON lines or JSON file
    with open(local_path, 'r', encoding='utf-8') as fh:
        jdata = fh.read()
    # naive: wrap as array if single object
    try:
        df = pd.read_json(local_path, lines=True)
    except ValueError:
        df = pd.json_normalize(pd.read_json(local_path))
    df['ingest_ts'] = datetime.utcnow().isoformat()
    df['source_file'] = local_path.name
    df['record_hash'] = df.apply(record_hash, axis=1)
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {STG_TABLE} (data VARIANT, ingest_ts TIMESTAMP_LTZ, source_file STRING, record_hash STRING)")
        for _, row in df.iterrows():
            row_json = row.drop(['ingest_ts','source_file','record_hash']).to_json(orient='records')
            cur.execute(
                f"INSERT INTO {STG_TABLE}(data, ingest_ts, source_file, record_hash) VALUES (parse_json(%s), to_timestamp_ltz(%s), %s, %s)",
                (row_json, row['ingest_ts'], row['source_file'], row['record_hash'])
            )
        conn.commit()
    finally:
        cur.close()


def merge_to_gold(conn):
    cur = conn.cursor()
    try:
        # simple merge example using record_hash to deduplicate
        cur.execute(f"CREATE TABLE IF NOT EXISTS {GOLD_TABLE} LIKE {STG_TABLE}")
        sql = f'''MERGE INTO {GOLD_TABLE} g
USING {STG_TABLE} s
ON g.record_hash = s.record_hash
WHEN NOT MATCHED THEN INSERT (data, ingest_ts, source_file, record_hash) VALUES (s.data, s.ingest_ts, s.source_file, s.record_hash)'''
        cur.execute(sql)
        conn.commit()
    finally:
        cur.close()


def main():
    conn = get_conn()
    try:
        csv_files = list(CSV_DIR.glob('*.csv'))
        json_files = list(JSON_DIR.glob('*.json'))
        LOG.info('Found %d csv and %d json files', len(csv_files), len(json_files))
        for f in csv_files:
            load_csv_to_staging(conn, f)
        for f in json_files:
            load_json_to_staging(conn, f)
        merge_to_gold(conn)
    finally:
        conn.close()

if __name__ == '__main__':
    main()

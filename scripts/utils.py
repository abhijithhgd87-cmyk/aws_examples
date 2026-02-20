from datetime import datetime
import hashlib
import pandas as pd


def add_audit_columns(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = df.copy()
    df['ingest_ts'] = datetime.utcnow().isoformat()
    df['source_file'] = source_file
    # compute record hash across all columns
    def row_hash(row):
        m = hashlib.sha256()
        concat = '|'.join([str(x) for x in row.values.tolist()])
        m.update(concat.encode('utf-8'))
        return m.hexdigest()
    df['record_hash'] = df.apply(row_hash, axis=1)
    return df

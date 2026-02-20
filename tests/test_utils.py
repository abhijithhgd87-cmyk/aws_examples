import pandas as pd
from scripts.utils import add_audit_columns

def test_add_audit_columns():
    df = pd.DataFrame({'a':[1,2],'b':['x','y']})
    out = add_audit_columns(df, 'file.csv')
    assert 'ingest_ts' in out.columns
    assert 'source_file' in out.columns
    assert 'record_hash' in out.columns
    assert all(out['source_file'] == 'file.csv')

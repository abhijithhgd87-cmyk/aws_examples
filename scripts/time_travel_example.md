Time Travel examples for recovery and cloning

1) Query a table as-of a past timestamp

```sql
SELECT * FROM my_schema.my_table AT (TIMESTAMP => '2026-02-19 12:00:00'::timestamp_ltz);
```

2) Create a clone at a point-in-time (fast, zero-copy snapshot)

```sql
CREATE TABLE my_table_clone CLONE my_schema.my_table AT (TIMESTAMP => '2026-02-19 12:00:00'::timestamp_ltz);
```

3) Recover a dropped table (if within retention window)

```sql
UNDROP TABLE my_schema.my_table;
```

Notes:
- Time Travel and Fail-safe retention vary by Snowflake edition and settings. Verify with your account admin.
- Clones are ideal for BI gold layer refresh tests — clone the staging/gold table, run transformations, then promote if OK.

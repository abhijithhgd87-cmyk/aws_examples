import argparse
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

#!/usr/bin/env python3
"""
SCD Type 1 implementation example using PySpark.

This script implements two simple SCD Type 1 strategies:
- full_replace (default): replace target rows with source rows for matching keys,
    and keep target rows whose keys are not present in source.
- column_level: update only specified columns when source values are non-null,
    otherwise keep existing target values.

Usage (spark-submit):
    spark-submit test.py --source /path/to/source.parquet --target /path/to/target.parquet \
        --keys id --mode full_replace --output /path/to/out.parquet

If no paths provided, the script runs a small self-contained example.
"""



def scd_type1_full_replace(source: DataFrame, target: DataFrame, key_cols: List[str]) -> DataFrame:
        """
        SCD Type 1 - full row replace:
            result = source U (target - target_keys_in_source)
        This means any row with a key present in source will be replaced by the source row.
        Rows in target whose key is not present in source are preserved.
        """
        # distinct keys from source (in case of duplicates)
        src_keys = source.select(*key_cols).distinct()
        # keep target rows whose key is not in source (left anti join)
        unchanged_target = target.join(src_keys, on=key_cols, how="left_anti")
        # final dataset: all source rows + preserved old target rows
        result = source.unionByName(unchanged_target, allowMissingColumns=True)
        return result


def scd_type1_column_level(
        source: DataFrame,
        target: DataFrame,
        key_cols: List[str],
        update_cols: Optional[List[str]] = None,
        prefer_source_when_null: bool = False,
) -> DataFrame:
        """
        SCD Type 1 - column level updates:
            - keys: used for join
            - update_cols: list of columns to update from source when source has a non-null value.
                If None, all non-key columns present in either DF will be considered.
            - prefer_source_when_null: if True, a present source row with null column value will
                overwrite target column with NULL. If False, NULL in source will be ignored and
                target value preserved.
        """
        # Build list of columns to consider for update
        all_cols = set(target.columns) | set(source.columns)
        # Remove key columns to find data columns
        data_cols = sorted(c for c in all_cols if c not in key_cols)
        if update_cols:
                data_cols = [c for c in data_cols if c in update_cols]

        # Alias dataframes
        s = source.alias("s")
        t = target.alias("t")

        # Perform full outer join to keep rows that exist only in target or only in source
        joined = t.join(s, on=key_cols, how="fullouter")

        # For each key column, coalesce t.key, s.key (they should be equal for matching rows)
        select_expr = []
        for k in key_cols:
                select_expr.append(F.coalesce(F.col(f"s.{k}"), F.col(f"t.{k}")).alias(k))

        # For each data column, choose value based on prefer_source_when_null and availability
        for c in data_cols:
                s_col = F.col(f"s.{c}")
                t_col = F.col(f"t.{c}")
                if prefer_source_when_null:
                        # If source row exists (not entirely null on keys), then source value (even if null) wins.
                        # Detect existence of source row by checking key non-null in s (coalesce(s.key,...))
                        # Simpler: if s_col is not null OR any s.key is not null: use s_col, else t_col.
                        # We'll check first key column as presence indicator (works when keys are non-null in source).
                        presence_cond = F.col(f"s.{key_cols[0]}").isNotNull()
                        chosen = F.when(presence_cond, s_col).otherwise(t_col)
                else:
                        # If source value is not null, take it; otherwise keep target value.
                        chosen = F.when(s_col.isNotNull(), s_col).otherwise(t_col)
                select_expr.append(chosen.alias(c))

        result = joined.select(*select_expr)
        return result


def _parse_args():
        parser = argparse.ArgumentParser(description="SCD Type 1 example with PySpark")
        parser.add_argument("--source", help="Source file (parquet or csv). If omitted, runs example data.")
        parser.add_argument("--target", help="Target file (parquet or csv). If omitted, runs example data.")
        parser.add_argument("--source-format", default="parquet", choices=["parquet", "csv"], help="Format of source file")
        parser.add_argument("--target-format", default="parquet", choices=["parquet", "csv"], help="Format of target file")
        parser.add_argument("--keys", required=False, help="Comma separated key columns (e.g. id).")
        parser.add_argument("--mode", default="full_replace", choices=["full_replace", "column_level"])
        parser.add_argument("--update-cols", help="Comma separated columns to update (only for column_level mode).")
        parser.add_argument("--prefer-source-when-null", action="store_true",
                                                help="In column_level mode, allow source NULL to overwrite target with NULL.")
        parser.add_argument("--output", help="Output path (parquet). If omitted prints top rows.")
        return parser.parse_args()


def _read_df(spark: SparkSession, path: str, fmt: str) -> DataFrame:
        if fmt == "parquet":
                return spark.read.parquet(path)
        elif fmt == "csv":
                return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        else:
                raise ValueError(f"unsupported format: {fmt}")


def main():
        args = _parse_args()
        spark = SparkSession.builder.appName("scd_type1_example").getOrCreate()

        if args.source and args.target:
                src_df = _read_df(spark, args.source, args.source_format)
                tgt_df = _read_df(spark, args.target, args.target_format)
                if not args.keys:
                        raise SystemExit("When providing files you must pass --keys")
                keys = [k.strip() for k in args.keys.split(",")]
        else:
                # Create a small example if no files provided
                # target has two rows; source updates one and inserts one new
                keys = ["id"]
                tgt_data = [
                        (1, "Alice", 30, "NY"),
                        (2, "Bob", 25, "CA"),
                        (3, "Charlie", 40, "TX")
                ]
                src_data = [
                        (2, "Bobby", 26, None),  # update Bob -> Bobby, age changed, state null (depending on mode)
                        (4, "Diana", 22, "WA")   # new row
                ]
                tgt_df = spark.createDataFrame(tgt_data, schema=["id", "name", "age", "state"])
                src_df = spark.createDataFrame(src_data, schema=["id", "name", "age", "state"])

        if args.mode == "full_replace":
                out_df = scd_type1_full_replace(src_df, tgt_df, keys)
        else:
                update_cols = None
                if args.update_cols:
                        update_cols = [c.strip() for c in args.update_cols.split(",")]
                out_df = scd_type1_column_level(
                        src_df,
                        tgt_df,
                        keys,
                        update_cols=update_cols,
                        prefer_source_when_null=args.prefer_source_when_null
                )

        if args.output:
                # write result as parquet
                out_df.write.mode("overwrite").parquet(args.output)
                print(f"Wrote result to {args.output}")
        else:
                # show small sample
                out_df.show(truncate=False)

        spark.stop()


if __name__ == "__main__":
        main()
"""Bronze ingestion helpers for Databricks and local PySpark runs."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_bronze_batch_csv(spark: SparkSession, source_glob_path: str) -> DataFrame:
    """Read CSV files in batch mode (local/dev) preserving raw source columns."""
    return spark.read.option("header", True).csv(source_glob_path)


def read_bronze_stream_cloudfiles(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
) -> DataFrame:
    """Read CSV files with Databricks Auto Loader (cloudFiles) in streaming mode."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", True)
        .load(source_path)
    )


def _record_hash_expr(raw_cols: Iterable[str]) -> F.Column:
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in raw_cols]
    return F.sha2(F.concat_ws("||", *cols), 256)


def add_ingestion_metadata(df: DataFrame, source_system: str, load_id: str) -> DataFrame:
    """Attach ingestion metadata while preserving all incoming raw columns."""
    raw_cols = list(df.columns)
    with_file = df.withColumn("source_file_name", F.input_file_name())
    with_batch_date = with_file.withColumn(
        "batch_date",
        F.regexp_extract(F.col("source_file_name"), r"batch_date=(\d{4}-\d{2}-\d{2})", 1),
    )
    return (
        with_batch_date.withColumn("source_system", F.lit(source_system))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("load_id", F.lit(load_id))
        .withColumn("record_hash", _record_hash_expr(raw_cols))
    )


def write_bronze_delta(df: DataFrame, table_name: str) -> None:
    """Persist Bronze raw table in Delta format using append semantics."""
    df.write.format("delta").mode("append").saveAsTable(table_name)

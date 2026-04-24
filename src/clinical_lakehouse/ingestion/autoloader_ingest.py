"""Auto Loader style ingestion patterns for Databricks."""

from pyspark.sql import DataFrame, SparkSession


def read_incremental_csv(
    spark: SparkSession,
    source_path: str,
    checkpoint_path: str,
    schema_location: str,
) -> DataFrame:
    """Read files incrementally using cloudFiles (Auto Loader style)."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("checkpointLocation", checkpoint_path)
        .load(source_path)
    )

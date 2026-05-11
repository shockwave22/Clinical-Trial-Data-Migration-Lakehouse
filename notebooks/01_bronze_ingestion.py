# Databricks notebook source
"""Bronze ingestion notebook for clinical migration raw tables."""

from datetime import datetime

from clinical_lakehouse.config.settings import BRONZE_RAW_TABLES
from clinical_lakehouse.ingestion.autoloader_ingest import (
    add_ingestion_metadata,
    read_bronze_batch_csv,
    read_bronze_stream_cloudfiles,
    write_bronze_delta,
)

# COMMAND ----------
# Local/batch-style ingestion for sample files in this repository.
# Reads all batches: data/sample_source/{system}/batch_date=YYYY-MM-DD/{system}.csv
load_id = datetime.utcnow().strftime("bronze_%Y%m%dT%H%M%SZ")

SOURCE_PATHS = {
    "oncore": "data/sample_source/oncore/batch_date=*/oncore.csv",
    "relisource": "data/sample_source/relisource/batch_date=*/relisource.csv",
    "great_plains": "data/sample_source/great_plains/batch_date=*/great_plains.csv",
    "sponsor_master": "data/sample_source/sponsor_master/batch_date=*/sponsor_master.csv",
}

for system_name, source_path in SOURCE_PATHS.items():
    raw_df = read_bronze_batch_csv(spark, source_path)
    bronze_df = add_ingestion_metadata(raw_df, source_system=system_name, load_id=load_id)
    write_bronze_delta(bronze_df, BRONZE_RAW_TABLES[system_name])

# COMMAND ----------
# Databricks Auto Loader (cloudFiles) streaming pattern (recommended in production):
#
# stream_df = read_bronze_stream_cloudfiles(
#     spark,
#     source_path="dbfs:/Volumes/clinical_migration_dev/raw/oncore/",
#     schema_location="dbfs:/Volumes/clinical_migration_dev/schemas/oncore_raw",
# )
# bronze_stream_df = add_ingestion_metadata(
#     stream_df,
#     source_system="oncore",
#     load_id=load_id,
# )
# (
#     bronze_stream_df.writeStream
#     .format("delta")
#     .option("checkpointLocation", "dbfs:/Volumes/clinical_migration_dev/checkpoints/bronze/oncore")
#     .outputMode("append")
#     .toTable("clinical_migration_dev.bronze.oncore_study_raw")
# )

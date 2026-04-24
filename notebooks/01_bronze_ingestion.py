# Databricks notebook source
"""Bronze ingestion notebook (PySpark-first)."""

from clinical_lakehouse.ingestion.autoloader_ingest import read_incremental_csv

# COMMAND ----------
# Example usage in Databricks:
# stream_df = read_incremental_csv(
#     spark,
#     source_path="/Volumes/ct_lakehouse_dev/raw/oncore/studies",
#     checkpoint_path="/Volumes/ct_lakehouse_dev/checkpoints/oncore_studies",
#     schema_location="/Volumes/ct_lakehouse_dev/schemas/oncore_studies",
# )
# stream_df.writeStream.format("delta").option("checkpointLocation", ...).toTable("ct_lakehouse_dev.bronze.oncore_studies")

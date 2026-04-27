# Databricks notebook source
"""Entity resolution notebook across OnCore, ReliSource, Great Plains, Sponsor Master."""

from clinical_lakehouse.transformations.entity_resolution import (
    build_entity_crosswalk,
    build_sponsor_crosswalk,
    build_study_contract_crosswalk,
    calculate_match_accuracy,
)

# COMMAND ----------
# Source tables (Bronze raw)
oncore_df = spark.table("clinical_migration_dev.bronze.oncore_study_raw")
relisource_df = spark.table("clinical_migration_dev.bronze.relisource_contract_raw")
great_plains_df = spark.table("clinical_migration_dev.bronze.gp_invoice_raw")
sponsor_master_df = spark.table("clinical_migration_dev.bronze.sponsor_master_raw")

# COMMAND ----------
# Build crosswalks
silver_sponsor_crosswalk = build_sponsor_crosswalk(
    sponsor_master=sponsor_master_df,
    oncore=oncore_df,
    relisource=relisource_df,
    great_plains=great_plains_df,
)

silver_study_contract_crosswalk = build_study_contract_crosswalk(
    oncore=oncore_df,
    relisource=relisource_df,
    great_plains=great_plains_df,
)

silver_entity_crosswalk = build_entity_crosswalk(
    sponsor_crosswalk=silver_sponsor_crosswalk,
    study_contract_crosswalk=silver_study_contract_crosswalk,
)

# COMMAND ----------
# Persist outputs
silver_sponsor_crosswalk.write.format("delta").mode("overwrite").saveAsTable(
    "clinical_migration_dev.silver.silver_sponsor_crosswalk"
)
silver_study_contract_crosswalk.write.format("delta").mode("overwrite").saveAsTable(
    "clinical_migration_dev.silver.silver_study_contract_crosswalk"
)
silver_entity_crosswalk.write.format("delta").mode("overwrite").saveAsTable(
    "clinical_migration_dev.silver.silver_entity_crosswalk"
)

# COMMAND ----------
# Validation data and accuracy
validation_df = spark.read.option("header", True).csv("data/sample_source/validation/entity_resolution_validation.csv")
validation_with_scores = validation_df.join(
    silver_study_contract_crosswalk.select("study_id", "contract_id", "overall_match_confidence"),
    on=["study_id", "contract_id"],
    how="left",
).fillna({"overall_match_confidence": 0.0})

# expected_match in CSV is TRUE/FALSE text
validation_with_scores = validation_with_scores.withColumn("expected_match", validation_with_scores.expected_match == "TRUE")
accuracy = calculate_match_accuracy(validation_with_scores, expected_col="expected_match")
print(f"Entity resolution accuracy: {accuracy:.4f}")
assert accuracy >= 0.97, "Accuracy target of 97% was not met"

# Databricks notebook source
"""Silver conformance notebook: standardization, DQ validation, dedupe, quarantine."""

from clinical_lakehouse.transformations.silver_transformations import (
    transform_silver_contract,
    transform_silver_invoice,
    transform_silver_site,
    transform_silver_sponsor,
    transform_silver_study,
)

# COMMAND ----------
# Read Bronze raw tables (Unity Catalog style)
bronze_study = spark.table("clinical_migration_dev.bronze.oncore_study_raw")
bronze_contract = spark.table("clinical_migration_dev.bronze.relisource_contract_raw")
bronze_invoice = spark.table("clinical_migration_dev.bronze.gp_invoice_raw")
bronze_sponsor = spark.table("clinical_migration_dev.bronze.sponsor_master_raw")

# COMMAND ----------
# Apply standardization + business keying + dedupe + invalid flagging
silver_study, q_study = transform_silver_study(bronze_study)
silver_contract, q_contract = transform_silver_contract(bronze_contract)
silver_invoice, q_invoice = transform_silver_invoice(bronze_invoice)
silver_sponsor, q_sponsor = transform_silver_sponsor(bronze_sponsor)
silver_site, q_site = transform_silver_site(bronze_study)

# COMMAND ----------
# Persist Silver valid tables
silver_study.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver.silver_study")
silver_contract.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver.silver_contract")
silver_invoice.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver.silver_invoice")
silver_sponsor.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver.silver_sponsor")
silver_site.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver.silver_site")

# COMMAND ----------
# Persist quarantine tables for rejected/invalid records
q_study.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver_quarantine.silver_study_rejected")
q_contract.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver_quarantine.silver_contract_rejected")
q_invoice.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver_quarantine.silver_invoice_rejected")
q_sponsor.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver_quarantine.silver_sponsor_rejected")
q_site.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.silver_quarantine.silver_site_rejected")

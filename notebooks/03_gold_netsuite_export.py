# Databricks notebook source
"""Gold notebook for NetSuite-ready canonical table generation."""

from clinical_lakehouse.exports.gold_netsuite_canonical import (
    build_gold_migration_audit_summary,
    build_gold_netsuite_contract_master,
    build_gold_netsuite_customer_master,
    build_gold_netsuite_invoice_header,
    build_gold_netsuite_invoice_line,
    build_gold_netsuite_open_ar,
    build_gold_netsuite_project_master,
)

# COMMAND ----------
# Read Silver and source tables
silver_study = spark.table("clinical_migration_dev.silver.silver_study")
silver_contract = spark.table("clinical_migration_dev.silver.silver_contract")
silver_invoice = spark.table("clinical_migration_dev.silver.silver_invoice")
silver_sponsor_crosswalk = spark.table("clinical_migration_dev.silver.silver_sponsor_crosswalk")
silver_study_contract_crosswalk = spark.table("clinical_migration_dev.silver.silver_study_contract_crosswalk")
sponsor_master_raw = spark.table("clinical_migration_dev.bronze.sponsor_master_raw")

# COMMAND ----------
# Build Gold canonical tables
customer_master = build_gold_netsuite_customer_master(silver_sponsor_crosswalk, sponsor_master_raw)
project_master = build_gold_netsuite_project_master(silver_study)
contract_master = build_gold_netsuite_contract_master(silver_contract, silver_study_contract_crosswalk)
invoice_header = build_gold_netsuite_invoice_header(silver_invoice, silver_study_contract_crosswalk)
invoice_line = build_gold_netsuite_invoice_line(invoice_header)
open_ar = build_gold_netsuite_open_ar(invoice_header)
audit_summary = build_gold_migration_audit_summary(
    customer_master,
    project_master,
    contract_master,
    invoice_header,
    invoice_line,
    open_ar,
)

# COMMAND ----------
# Persist Gold NetSuite-ready targets
customer_master.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_customer_master")
project_master.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_project_master")
contract_master.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_contract_master")
invoice_header.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_invoice_header")
invoice_line.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_invoice_line")
open_ar.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_netsuite_open_ar")
audit_summary.write.format("delta").mode("overwrite").saveAsTable("clinical_migration_dev.gold.gold_migration_audit_summary")

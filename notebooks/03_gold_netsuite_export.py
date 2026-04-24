# Databricks notebook source
"""Gold notebook for reconciliation and NetSuite export."""

from clinical_lakehouse.exports.netsuite_export import build_netsuite_invoice_export
from clinical_lakehouse.reconciliation.reporting import build_ar_reconciliation

# COMMAND ----------
# recon_df = build_ar_reconciliation(invoices_df, payments_df)
# export_df = build_netsuite_invoice_export(recon_df)
# export_df.write.format("delta").mode("overwrite").saveAsTable("ct_lakehouse_dev.gold.netsuite_invoice_export")

"""NetSuite-ready export transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_netsuite_invoice_export(recon_df: DataFrame) -> DataFrame:
    """Shape reconciliation output into NetSuite-style invoice export table."""
    return recon_df.select(
        F.col("sponsor_name").alias("customer_name"),
        F.col("study_name").alias("project_name"),
        F.col("billed_amount").alias("invoice_total"),
        F.col("paid_amount").alias("amount_paid"),
        F.col("outstanding_amount").alias("amount_due"),
    )

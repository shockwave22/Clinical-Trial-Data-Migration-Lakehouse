"""Financial reconciliation outputs for Gold layer."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_ar_reconciliation(invoices: DataFrame, payments: DataFrame) -> DataFrame:
    """Compute billed, paid, and outstanding by sponsor and study."""
    inv = invoices.groupBy("sponsor_name", "study_name").agg(F.sum("invoice_amount").alias("billed_amount"))
    pay = payments.groupBy("sponsor_name", "study_name").agg(F.sum("payment_amount").alias("paid_amount"))
    return (
        inv.join(pay, on=["sponsor_name", "study_name"], how="left")
        .fillna({"paid_amount": 0.0})
        .withColumn("outstanding_amount", F.col("billed_amount") - F.col("paid_amount"))
    )

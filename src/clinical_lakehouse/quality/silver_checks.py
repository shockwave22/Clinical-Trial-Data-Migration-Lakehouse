"""Silver data quality checks requested by migration requirements."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def study_protocol_required(df: DataFrame) -> DataFrame:
    return df.filter(F.trim(F.coalesce(F.col("protocol_no_std"), F.lit(""))) == "")


def sponsor_external_id_required(df: DataFrame) -> DataFrame:
    return df.filter(F.trim(F.coalesce(F.col("sponsor_external_id_std"), F.lit(""))) == "")


def invoice_amount_gte_paid(df: DataFrame) -> DataFrame:
    return df.filter(F.col("invoice_amount_std") < F.col("paid_amount_std"))


def outstanding_matches_formula(df: DataFrame) -> DataFrame:
    return df.filter(F.round(F.col("invoice_amount_std") - F.col("paid_amount_std"), 2) != F.col("outstanding_amount_std"))


def contract_value_positive(df: DataFrame) -> DataFrame:
    return df.filter(F.col("contract_value_std") <= 0)

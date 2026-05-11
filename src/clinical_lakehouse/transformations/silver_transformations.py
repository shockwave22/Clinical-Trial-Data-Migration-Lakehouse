"""Silver layer standardization, business keying, deduplication, and quarantine splits."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def normalize_text(col_name: str) -> F.Column:
    return F.upper(F.trim(F.regexp_replace(F.col(col_name).cast("string"), r"\s+", " ")))


def normalize_token(col_name: str) -> F.Column:
    return F.regexp_replace(normalize_text(col_name), r"[^A-Z0-9]", "")


def to_date_safe(col_name: str) -> F.Column:
    return F.to_date(F.col(col_name).cast("string"), "yyyy-MM-dd")


def to_currency(col_name: str) -> F.Column:
    return F.round(F.col(col_name).cast("double"), 2)


def deduplicate_by_key(df: DataFrame, key_col: str) -> DataFrame:
    order_col = F.col("ingestion_timestamp").desc_nulls_last() if "ingestion_timestamp" in df.columns else F.lit(1)
    w = Window.partitionBy(key_col).orderBy(order_col)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _with_invalid_reason(df: DataFrame, reason_col: str = "invalid_reason") -> DataFrame:
    return df.withColumn(reason_col, F.trim(F.regexp_replace(F.col(reason_col), r"^\|+|\|+$", "")))


def transform_silver_study(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        bronze_df.withColumn("study_name_std", normalize_text("study_name"))
        .withColumn("protocol_no_std", normalize_token("protocol_no"))
        .withColumn("sponsor_external_id_std", normalize_token("sponsor_external_id"))
        .withColumn("site_code_std", normalize_token("site_code"))
        .withColumn("start_date_std", to_date_safe("start_date"))
        .withColumn("end_date_std", to_date_safe("end_date"))
        .withColumn("business_key", F.concat_ws("|", F.col("study_name_std"), F.col("protocol_no_std"), F.col("site_code_std")))
        .withColumn(
            "invalid_reason",
            F.concat_ws(
                "|",
                F.when(F.col("protocol_no_std") == "", F.lit("protocol_no_required")),
                F.when(F.col("sponsor_external_id_std") == "", F.lit("sponsor_external_id_required")),
            ),
        )
    )
    deduped = deduplicate_by_key(df, "business_key")
    rejected = _with_invalid_reason(deduped.filter(F.col("invalid_reason") != ""))
    valid = deduped.filter(F.col("invalid_reason") == "")
    return valid, rejected


def transform_silver_contract(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        bronze_df.withColumn("contract_name_std", normalize_token("contract_name"))
        .withColumn("study_name_std", normalize_text("study_name"))
        .withColumn("protocol_no_std", normalize_token("protocol_no"))
        .withColumn("sponsor_external_id_std", normalize_token("sponsor_external_id"))
        .withColumn("effective_date_std", to_date_safe("effective_date"))
        .withColumn("contract_value_std", to_currency("contract_value"))
        .withColumn("business_key", F.concat_ws("|", F.col("contract_name_std"), F.col("protocol_no_std"), F.col("sponsor_external_id_std")))
        .withColumn(
            "invalid_reason",
            F.concat_ws(
                "|",
                F.when(F.col("sponsor_external_id_std") == "", F.lit("sponsor_external_id_required")),
                F.when(F.col("contract_value_std") <= 0, F.lit("contract_value_must_be_positive")),
            ),
        )
    )
    deduped = deduplicate_by_key(df, "business_key")
    rejected = _with_invalid_reason(deduped.filter(F.col("invalid_reason") != ""))
    valid = deduped.filter(F.col("invalid_reason") == "")
    return valid, rejected


def transform_silver_invoice(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        bronze_df.withColumn("contract_name_std", normalize_token("contract_name"))
        .withColumn("protocol_no_std", normalize_token("protocol_no"))
        .withColumn("sponsor_external_id_std", normalize_token("sponsor_external_id"))
        .withColumn("invoice_date_std", to_date_safe("invoice_date"))
        .withColumn("invoice_amount_std", to_currency("invoice_amount"))
        .withColumn("paid_amount_std", to_currency("paid_amount"))
        .withColumn("outstanding_amount_std", to_currency("outstanding_amount"))
        .withColumn("business_key", F.concat_ws("|", normalize_token("invoice_id"), normalize_token("invoice_number")))
        .withColumn("calc_outstanding", F.round(F.col("invoice_amount_std") - F.col("paid_amount_std"), 2))
        .withColumn(
            "invalid_reason",
            F.concat_ws(
                "|",
                F.when(F.col("sponsor_external_id_std") == "", F.lit("sponsor_external_id_required")),
                F.when(F.col("invoice_amount_std") < F.col("paid_amount_std"), F.lit("invoice_amount_lt_paid_amount")),
                F.when(F.col("outstanding_amount_std") != F.col("calc_outstanding"), F.lit("outstanding_mismatch")),
            ),
        )
    )
    deduped = deduplicate_by_key(df, "business_key").drop("calc_outstanding")
    rejected = _with_invalid_reason(deduped.filter(F.col("invalid_reason") != ""))
    valid = deduped.filter(F.col("invalid_reason") == "")
    return valid, rejected


def transform_silver_sponsor(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        bronze_df.withColumn("sponsor_name_std", normalize_text("sponsor_name"))
        .withColumn("sponsor_external_id_std", normalize_token("sponsor_external_id"))
        .withColumn("business_key", F.col("sponsor_external_id_std"))
        .withColumn(
            "invalid_reason",
            F.concat_ws("|", F.when(F.col("sponsor_external_id_std") == "", F.lit("sponsor_external_id_required"))),
        )
    )
    deduped = deduplicate_by_key(df, "business_key")
    rejected = _with_invalid_reason(deduped.filter(F.col("invalid_reason") != ""))
    valid = deduped.filter(F.col("invalid_reason") == "")
    return valid, rejected


def transform_silver_site(bronze_study_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        bronze_study_df.withColumn("site_code_std", normalize_token("site_code"))
        .withColumn("protocol_no_std", normalize_token("protocol_no"))
        .withColumn("study_name_std", normalize_text("study_name"))
        .withColumn("sponsor_external_id_std", normalize_token("sponsor_external_id"))
        .withColumn("business_key", F.concat_ws("|", F.col("site_code_std"), F.col("study_name_std")))
        .withColumn(
            "invalid_reason",
            F.concat_ws(
                "|",
                F.when(F.col("site_code_std") == "", F.lit("site_code_required")),
                F.when(F.col("sponsor_external_id_std") == "", F.lit("sponsor_external_id_required")),
            ),
        )
    )
    deduped = deduplicate_by_key(df, "business_key")
    rejected = _with_invalid_reason(deduped.filter(F.col("invalid_reason") != ""))
    valid = deduped.filter(F.col("invalid_reason") == "")
    return valid, rejected

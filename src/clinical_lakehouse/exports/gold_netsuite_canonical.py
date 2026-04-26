"""Gold layer canonical NetSuite-ready table builders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_gold_netsuite_customer_master(silver_sponsor_crosswalk: DataFrame, sponsor_master_raw: DataFrame) -> DataFrame:
    """Map Sponsor to NetSuite Customer master."""
    sponsor = sponsor_master_raw.select(
        "sponsor_external_id",
        "sponsor_name",
        "sponsor_parent",
        "billing_account_id",
        "netsuite_customer_external_id",
        "sponsor_status",
    ).dropDuplicates(["sponsor_external_id"])

    return (
        silver_sponsor_crosswalk.join(
            sponsor,
            silver_sponsor_crosswalk.master_sponsor_external_id == sponsor.sponsor_external_id,
            "left",
        )
        .select(
            F.col("master_sponsor_external_id").alias("customer_external_id"),
            F.col("netsuite_customer_external_id"),
            F.col("sponsor_name").alias("customer_name"),
            F.col("sponsor_parent").alias("parent_customer"),
            F.col("billing_account_id"),
            F.col("sponsor_status").alias("customer_status"),
            F.col("overall_match_confidence").alias("match_confidence"),
        )
        .dropDuplicates(["customer_external_id"])
    )


def build_gold_netsuite_project_master(silver_study: DataFrame) -> DataFrame:
    """Map study/protocol into NetSuite project master."""
    return silver_study.select(
        F.col("study_id").alias("project_external_id"),
        F.col("study_name_std").alias("project_name"),
        F.col("protocol_no_std").alias("protocol_no"),
        F.col("sponsor_external_id_std").alias("customer_external_id"),
        F.col("site_code_std").alias("primary_site_code"),
        F.col("start_date_std").alias("project_start_date"),
        F.col("end_date_std").alias("project_end_date"),
        F.col("study_status").alias("project_status"),
    ).dropDuplicates(["project_external_id"])


def build_gold_netsuite_contract_master(silver_contract: DataFrame, study_contract_crosswalk: DataFrame) -> DataFrame:
    """Map ReliSource contracts into NetSuite contract master."""
    return (
        silver_contract.alias("c")
        .join(study_contract_crosswalk.alias("x"), F.col("c.contract_id") == F.col("x.contract_id"), "left")
        .select(
            F.col("c.contract_id").alias("contract_external_id"),
            F.col("c.contract_name_std").alias("contract_name"),
            F.col("c.sponsor_external_id_std").alias("customer_external_id"),
            F.col("x.study_id").alias("project_external_id"),
            F.col("c.protocol_no_std").alias("protocol_no"),
            F.col("c.contract_status").alias("contract_status"),
            F.col("c.contract_value_std").alias("contract_value"),
            F.col("c.effective_date_std").alias("effective_date"),
            F.col("c.amendment_no").alias("amendment_no"),
        )
        .dropDuplicates(["contract_external_id"])
    )


def build_gold_netsuite_invoice_header(silver_invoice: DataFrame, study_contract_crosswalk: DataFrame) -> DataFrame:
    """Map Great Plains invoices to NetSuite invoice header."""
    mapped = silver_invoice.alias("i").join(
        study_contract_crosswalk.alias("x"),
        (F.col("i.invoice_id") == F.col("x.invoice_id")) | (F.col("i.protocol_no") == F.col("x.protocol_no")),
        "left",
    )

    return mapped.select(
        F.col("i.invoice_id").alias("invoice_external_id"),
        F.col("i.invoice_number").alias("invoice_number"),
        F.col("x.contract_id").alias("contract_external_id"),
        F.col("x.study_id").alias("project_external_id"),
        F.col("i.sponsor_external_id_std").alias("customer_external_id"),
        F.col("i.invoice_date_std").alias("invoice_date"),
        F.col("i.payment_status").alias("payment_status"),
        F.col("i.invoice_amount_std").alias("invoice_total"),
        F.col("i.paid_amount_std").alias("amount_paid"),
        F.col("i.outstanding_amount_std").alias("amount_due"),
    ).dropDuplicates(["invoice_external_id"])


def build_gold_netsuite_invoice_line(invoice_header: DataFrame) -> DataFrame:
    """Create invoice line records from header-level amounts (single-line mock)."""
    return invoice_header.select(
        "invoice_external_id",
        F.lit(1).alias("line_number"),
        F.coalesce(F.col("project_external_id"), F.lit("UNMAPPED")).alias("project_external_id"),
        F.coalesce(F.col("contract_external_id"), F.lit("UNMAPPED")).alias("contract_external_id"),
        F.col("invoice_total").alias("line_amount"),
        F.lit("Clinical Trial Billing").alias("line_description"),
    )


def build_gold_netsuite_open_ar(invoice_header: DataFrame) -> DataFrame:
    """Build open AR from invoice headers where amount due is positive."""
    return invoice_header.filter(F.col("amount_due") > 0).select(
        "invoice_external_id",
        "invoice_number",
        "customer_external_id",
        "project_external_id",
        "contract_external_id",
        "invoice_date",
        "amount_due",
        "payment_status",
    )


def build_gold_migration_audit_summary(
    customer_master: DataFrame,
    project_master: DataFrame,
    contract_master: DataFrame,
    invoice_header: DataFrame,
    invoice_line: DataFrame,
    open_ar: DataFrame,
) -> DataFrame:
    """Create audit counts and financial totals for migration readiness."""
    spark = customer_master.sparkSession
    rows = [
        ("gold_netsuite_customer_master", customer_master.count(), None),
        ("gold_netsuite_project_master", project_master.count(), None),
        ("gold_netsuite_contract_master", contract_master.count(), None),
        ("gold_netsuite_invoice_header", invoice_header.count(), float(invoice_header.agg(F.sum("invoice_total")).collect()[0][0] or 0.0)),
        ("gold_netsuite_invoice_line", invoice_line.count(), float(invoice_line.agg(F.sum("line_amount")).collect()[0][0] or 0.0)),
        ("gold_netsuite_open_ar", open_ar.count(), float(open_ar.agg(F.sum("amount_due")).collect()[0][0] or 0.0)),
    ]
    return spark.createDataFrame(rows, ["table_name", "record_count", "amount_total"]).withColumn(
        "audit_timestamp", F.current_timestamp()
    )

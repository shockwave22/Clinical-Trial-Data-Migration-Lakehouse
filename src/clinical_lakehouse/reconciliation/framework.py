"""Reconciliation framework for migration quality and financial control metrics."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _single_metric_row(spark: SparkSession, metric_name: str, metric_value: float, metric_group: str) -> DataFrame:
    return spark.createDataFrame([(metric_group, metric_name, float(metric_value))], ["metric_group", "metric_name", "metric_value"])


def source_vs_target_record_count(source_df: DataFrame, target_df: DataFrame, metric_group: str) -> DataFrame:
    spark = source_df.sparkSession
    source_count = source_df.count()
    target_count = target_df.count()
    return spark.createDataFrame(
        [
            (metric_group, "source_record_count", float(source_count)),
            (metric_group, "target_record_count", float(target_count)),
            (metric_group, "record_count_delta", float(target_count - source_count)),
        ],
        ["metric_group", "metric_name", "metric_value"],
    )


def source_vs_target_total(source_df: DataFrame, source_col: str, target_df: DataFrame, target_col: str, metric_group: str) -> DataFrame:
    spark = source_df.sparkSession
    source_total = float(source_df.agg(F.sum(F.col(source_col))).collect()[0][0] or 0.0)
    target_total = float(target_df.agg(F.sum(F.col(target_col))).collect()[0][0] or 0.0)
    return spark.createDataFrame(
        [
            (metric_group, "source_total", source_total),
            (metric_group, "target_total", target_total),
            (metric_group, "total_delta", target_total - source_total),
        ],
        ["metric_group", "metric_name", "metric_value"],
    )


def rejected_record_count(rejected_df: DataFrame, metric_group: str = "quality") -> DataFrame:
    return _single_metric_row(rejected_df.sparkSession, "rejected_record_count", rejected_df.count(), metric_group)


def duplicate_count(df: DataFrame, key_cols: list[str], metric_group: str = "quality") -> DataFrame:
    dup = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1).alias("duplicate_count"))
        .collect()[0][0]
        or 0
    )
    return _single_metric_row(df.sparkSession, "duplicate_count", float(dup), metric_group)


def match_accuracy(crosswalk_df: DataFrame, confidence_col: str = "overall_match_confidence", threshold: float = 0.97) -> DataFrame:
    spark = crosswalk_df.sparkSession
    total = crosswalk_df.count()
    high_conf = crosswalk_df.filter(F.col(confidence_col) >= threshold).count()
    accuracy = float(high_conf / total) if total else 0.0
    return spark.createDataFrame(
        [
            ("entity_resolution", "high_confidence_match_count", float(high_conf)),
            ("entity_resolution", "total_match_count", float(total)),
            ("entity_resolution", "match_accuracy", accuracy),
        ],
        ["metric_group", "metric_name", "metric_value"],
    )


def load_runtime_seconds(load_start_ts: datetime, load_end_ts: datetime) -> float:
    return float((load_end_ts - load_start_ts).total_seconds())


def change_type_counts(delta_df: DataFrame, metric_group: str = "delta_load") -> DataFrame:
    counts = {r["change_type"]: r["count"] for r in delta_df.groupBy("change_type").count().collect()}
    spark = delta_df.sparkSession
    return spark.createDataFrame(
        [
            (metric_group, "insert_count", float(counts.get("insert", 0))),
            (metric_group, "update_count", float(counts.get("update", 0))),
            (metric_group, "delete_count", float(counts.get("delete", 0))),
            (metric_group, "no_change_count", float(counts.get("no_change", 0))),
        ],
        ["metric_group", "metric_name", "metric_value"],
    )


def assemble_reconciliation_report(*metric_dfs: DataFrame) -> DataFrame:
    report = metric_dfs[0]
    for df in metric_dfs[1:]:
        report = report.unionByName(df)
    return report.withColumn("report_generated_ts", F.current_timestamp())


def runtime_metric_df(spark: SparkSession, load_start_ts: datetime, load_end_ts: datetime) -> DataFrame:
    runtime_sec = load_runtime_seconds(load_start_ts, load_end_ts)
    return _single_metric_row(spark, "load_runtime_seconds", runtime_sec, "runtime")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

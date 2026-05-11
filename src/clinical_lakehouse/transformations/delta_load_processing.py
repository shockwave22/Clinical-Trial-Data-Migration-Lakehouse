"""Incremental delta load processing for Silver/Gold tables with SCD2 support."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CHANGE_INSERT = "insert"
CHANGE_UPDATE = "update"
CHANGE_DELETE = "delete"
CHANGE_NO_CHANGE = "no_change"


def detect_delta_changes(
    source_df: DataFrame,
    target_df: DataFrame,
    key_cols: Iterable[str],
    hash_col: str = "record_hash",
) -> DataFrame:
    """Detect inserts/updates/deletes/no-change using full outer join and record_hash."""
    keys = list(key_cols)
    src = source_df.alias("s")
    tgt = target_df.alias("t")
    join_cond = [F.col(f"s.{k}") == F.col(f"t.{k}") for k in keys]

    joined = src.join(tgt, on=join_cond, how="fullouter")

    business_key = F.coalesce(*[F.col(f"s.{k}") for k in keys], *[F.col(f"t.{k}") for k in keys])

    return (
        joined.withColumn("_source_exists", F.col(f"s.{keys[0]}").isNotNull())
        .withColumn("_target_exists", F.col(f"t.{keys[0]}").isNotNull())
        .withColumn(
            "change_type",
            F.when(F.col("_source_exists") & ~F.col("_target_exists"), F.lit(CHANGE_INSERT))
            .when(~F.col("_source_exists") & F.col("_target_exists"), F.lit(CHANGE_DELETE))
            .when(F.col(f"s.{hash_col}") != F.col(f"t.{hash_col}"), F.lit(CHANGE_UPDATE))
            .otherwise(F.lit(CHANGE_NO_CHANGE)),
        )
        .withColumn("delta_business_key", business_key)
        .withColumn("delta_detected_at", F.current_timestamp())
    )


def merge_delta_current(
    spark: SparkSession,
    table_name: str,
    staged_df: DataFrame,
    key_cols: Iterable[str],
    hash_col: str = "record_hash",
) -> None:
    """Merge staged source rows into current table, applying insert/update/delete semantics."""
    keys = list(key_cols)
    merge_cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    delta_table = DeltaTable.forName(spark, table_name)

    source_for_merge = staged_df.filter(F.col("change_type").isin(CHANGE_INSERT, CHANGE_UPDATE, CHANGE_DELETE))

    (
        delta_table.alias("t")
        .merge(source_for_merge.alias("s"), merge_cond)
        .whenMatchedDelete(condition="s.change_type = 'delete'")
        .whenMatchedUpdate(
            condition=f"s.change_type = 'update' AND t.{hash_col} <> s.{hash_col}",
            set={c: f"s.{c}" for c in staged_df.columns if c not in {"change_type", "delta_business_key", "delta_detected_at"}},
        )
        .whenNotMatchedInsert(
            condition="s.change_type = 'insert'",
            values={c: f"s.{c}" for c in staged_df.columns if c not in {"change_type", "delta_business_key", "delta_detected_at"}},
        )
        .execute()
    )


def merge_scd2_history(
    spark: SparkSession,
    history_table_name: str,
    staged_df: DataFrame,
    business_key_col: str,
    hash_col: str = "record_hash",
    effective_ts_col: str = "effective_from_ts",
    expire_ts_col: str = "effective_to_ts",
    is_current_col: str = "is_current",
) -> None:
    """SCD Type 2 merge: close current rows on update/delete and insert new current rows on insert/update."""
    delta_table = DeltaTable.forName(spark, history_table_name)

    changes = staged_df.filter(F.col("change_type").isin(CHANGE_INSERT, CHANGE_UPDATE, CHANGE_DELETE))

    # Close currently active record for updates/deletes
    (
        delta_table.alias("t")
        .merge(changes.alias("s"), f"t.{business_key_col} = s.{business_key_col} AND t.{is_current_col} = true")
        .whenMatchedUpdate(
            condition="s.change_type IN ('update','delete')",
            set={
                is_current_col: "false",
                expire_ts_col: "current_timestamp()",
            },
        )
        .execute()
    )

    # Insert new current version for inserts/updates
    to_insert = (
        changes.filter(F.col("change_type").isin(CHANGE_INSERT, CHANGE_UPDATE))
        .withColumn(effective_ts_col, F.current_timestamp())
        .withColumn(expire_ts_col, F.lit(None).cast("timestamp"))
        .withColumn(is_current_col, F.lit(True))
    )

    (
        delta_table.alias("t")
        .merge(to_insert.alias("s"), f"t.{business_key_col} = s.{business_key_col} AND t.{is_current_col} = true")
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in to_insert.columns})
        .execute()
    )


def generate_netsuite_delta_export(delta_df: DataFrame, base_output_path: str, entity_name: str, batch_id: str) -> str:
    """Write change rows (excluding no_change) as batch delta export CSV."""
    output_path = f"{base_output_path}/{entity_name}_delta/batch_id={batch_id}"
    (
        delta_df.filter(F.col("change_type") != CHANGE_NO_CHANGE)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    return output_path


def default_batch_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

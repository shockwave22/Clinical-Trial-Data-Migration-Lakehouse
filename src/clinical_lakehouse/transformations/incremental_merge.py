"""Reusable Delta MERGE helper for upserts."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def merge_on_keys(
    target_table: str,
    source_df: DataFrame,
    merge_condition: str,
    hash_col: str = "record_hash",
) -> None:
    """Merge records into Delta target using hash-based change detection."""
    delta_target = DeltaTable.forName(source_df.sparkSession, target_table)
    (
        delta_target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition=f"t.{hash_col} <> s.{hash_col}",
            set={c: f"s.{c}" for c in source_df.columns},
        )
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in source_df.columns})
        .execute()
    )

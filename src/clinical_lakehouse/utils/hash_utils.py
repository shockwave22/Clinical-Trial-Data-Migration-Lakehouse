"""Hash utilities for deterministic change detection."""

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_record_hash(df: DataFrame, cols: Iterable[str], out_col: str = "record_hash") -> DataFrame:
    """Create a SHA-256 hash across selected columns for merge-based incremental loads."""
    safe_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return df.withColumn(out_col, F.sha2(F.concat_ws("||", *safe_cols), 256))

"""Data quality checks and threshold reporting."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def required_field_violations(df: DataFrame, required_cols: list[str]) -> DataFrame:
    """Return counts of null/blank violations for required columns."""
    checks = [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
        for c in required_cols
    ]
    return df.agg(*checks)


def referential_integrity_orphans(child: DataFrame, parent: DataFrame, key: str) -> int:
    """Count child records that do not map to parent key."""
    return child.join(parent.select(key).dropDuplicates(), on=key, how="left_anti").count()

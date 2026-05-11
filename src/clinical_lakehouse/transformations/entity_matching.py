"""Entity matching utilities across CTMS/contracts/finance sources."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_join_keys(df: DataFrame, cols: list[str]) -> DataFrame:
    """Normalize keys by trimming, upper-casing, and removing punctuation."""
    out = df
    for c in cols:
        out = out.withColumn(
            c,
            F.regexp_replace(F.upper(F.trim(F.col(c))), r"[^A-Z0-9]", ""),
        )
    return out


def build_study_contract_link(studies: DataFrame, contracts: DataFrame) -> DataFrame:
    """Deterministically match studies to contracts using protocol + sponsor keys."""
    s = normalize_join_keys(studies, ["protocol_number", "study_name", "sponsor_name"])
    c = normalize_join_keys(contracts, ["protocol_number", "contract_name", "sponsor_name"])
    return s.join(
        c,
        on=["protocol_number", "sponsor_name"],
        how="left",
    )

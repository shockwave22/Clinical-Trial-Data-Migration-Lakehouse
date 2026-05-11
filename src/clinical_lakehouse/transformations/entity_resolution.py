"""Entity resolution across OnCore, ReliSource, Great Plains, and Sponsor Master."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def normalize_token(col_name: str) -> F.Column:
    return F.regexp_replace(F.upper(F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))), r"[^A-Z0-9]", "")


def normalize_text(col_name: str) -> F.Column:
    return F.upper(F.trim(F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"\s+", " ")))


def similarity_score(left: F.Column, right: F.Column) -> F.Column:
    max_len = F.greatest(F.length(left), F.length(right))
    dist = F.levenshtein(left, right)
    return F.when(max_len == 0, F.lit(1.0)).otherwise(F.round(1 - (dist / max_len), 4))


def with_standardized_keys(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    out = df
    for raw_col, std_col in mapping.items():
        if std_col in {"sponsor_name_std", "study_name_std", "contract_name_std"}:
            out = out.withColumn(std_col, normalize_text(raw_col))
        else:
            out = out.withColumn(std_col, normalize_token(raw_col))
    return out


def _scored_candidates(left: DataFrame, right: DataFrame, join_condition: F.Column) -> DataFrame:
    joined = left.alias("l").join(right.alias("r"), join_condition, "inner")

    sponsor_exact = (F.col("l.sponsor_external_id_std") != "") & (
        F.col("l.sponsor_external_id_std") == F.col("r.sponsor_external_id_std")
    )
    protocol_exact = (F.col("l.protocol_no_std") != "") & (F.col("l.protocol_no_std") == F.col("r.protocol_no_std"))
    contract_exact = (F.col("l.contract_name_std") != "") & (F.col("l.contract_name_std") == F.col("r.contract_name_std"))
    study_exact = (F.col("l.study_name_std") != "") & (F.col("l.study_name_std") == F.col("r.study_name_std"))

    sponsor_fuzzy = similarity_score(F.col("l.sponsor_name_std"), F.col("r.sponsor_name_std"))
    contract_fuzzy = similarity_score(F.col("l.contract_name_std"), F.col("r.contract_name_std"))

    ranked = (
        joined.withColumn(
            "match_level",
            F.when(sponsor_exact, F.lit(1))
            .when(protocol_exact, F.lit(2))
            .when(contract_exact, F.lit(3))
            .when(study_exact, F.lit(4))
            .when((sponsor_fuzzy >= 0.90) & (contract_fuzzy >= 0.85), F.lit(5))
            .otherwise(F.lit(99)),
        )
        .withColumn("sponsor_match_score", F.when(sponsor_exact, 1.0).otherwise(sponsor_fuzzy))
        .withColumn(
            "study_match_score",
            F.when(study_exact, 1.0).otherwise(similarity_score(F.col("l.study_name_std"), F.col("r.study_name_std"))),
        )
        .withColumn("contract_match_score", F.when(contract_exact, 1.0).otherwise(contract_fuzzy))
        .withColumn(
            "overall_match_confidence",
            F.round(
                F.col("sponsor_match_score") * 0.45
                + F.col("study_match_score") * 0.25
                + F.col("contract_match_score") * 0.30,
                4,
            ),
        )
    )

    return ranked.filter(F.col("match_level") <= 5)


def build_sponsor_crosswalk(sponsor_master: DataFrame, oncore: DataFrame, relisource: DataFrame, great_plains: DataFrame) -> DataFrame:
    sponsor_std = with_standardized_keys(
        sponsor_master,
        {
            "sponsor_external_id": "sponsor_external_id_std",
            "sponsor_name": "sponsor_name_std",
        },
    ).select("sponsor_external_id", "sponsor_external_id_std", "sponsor_name", "sponsor_name_std", "billing_account_id")

    source_union = (
        oncore.select("sponsor_external_id", "study_name")
        .withColumn("contract_name", F.lit(""))
        .withColumn("sponsor_name", F.lit(""))
        .unionByName(relisource.select("sponsor_external_id", "study_name", "contract_name", F.lit("").alias("sponsor_name")))
        .unionByName(great_plains.select("sponsor_external_id", F.lit("").alias("study_name"), "contract_name", F.lit("").alias("sponsor_name")))
        .dropDuplicates()
    )

    source_std = with_standardized_keys(
        source_union,
        {
            "sponsor_external_id": "sponsor_external_id_std",
            "study_name": "study_name_std",
            "contract_name": "contract_name_std",
            "sponsor_name": "sponsor_name_std",
        },
    )

    candidates = _scored_candidates(
        source_std,
        sponsor_std.withColumn("protocol_no_std", F.lit(""))
        .withColumn("study_name_std", F.lit(""))
        .withColumn("contract_name_std", F.lit("")),
        (F.col("l.sponsor_external_id_std") == F.col("r.sponsor_external_id_std")) | (F.col("l.sponsor_external_id_std") != ""),
    )

    w = Window.partitionBy("l.sponsor_external_id").orderBy(F.col("match_level").asc(), F.col("overall_match_confidence").desc())
    return (
        candidates.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("l.sponsor_external_id").alias("source_sponsor_external_id"),
            F.col("r.sponsor_external_id").alias("master_sponsor_external_id"),
            F.col("r.billing_account_id"),
            "sponsor_match_score",
            "study_match_score",
            "contract_match_score",
            "overall_match_confidence",
            "match_level",
        )
    )


def build_study_contract_crosswalk(oncore: DataFrame, relisource: DataFrame, great_plains: DataFrame) -> DataFrame:
    oncore_std = with_standardized_keys(
        oncore,
        {
            "study_id": "study_id_std",
            "study_name": "study_name_std",
            "protocol_no": "protocol_no_std",
            "sponsor_external_id": "sponsor_external_id_std",
        },
    ).withColumn("contract_name_std", F.lit("")).withColumn("sponsor_name_std", F.lit(""))

    reli_std = with_standardized_keys(
        relisource,
        {
            "contract_id": "contract_id_std",
            "contract_name": "contract_name_std",
            "study_name": "study_name_std",
            "protocol_no": "protocol_no_std",
            "sponsor_external_id": "sponsor_external_id_std",
        },
    ).withColumn("sponsor_name_std", F.lit(""))

    candidates = _scored_candidates(
        oncore_std,
        reli_std,
        (F.col("l.sponsor_external_id_std") == F.col("r.sponsor_external_id_std"))
        | (F.col("l.protocol_no_std") == F.col("r.protocol_no_std"))
        | (F.col("l.study_name_std") == F.col("r.study_name_std")),
    )

    w = Window.partitionBy("l.study_id").orderBy(F.col("match_level").asc(), F.col("overall_match_confidence").desc())
    crosswalk = (
        candidates.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("l.study_id").alias("study_id"),
            F.col("l.study_name").alias("study_name"),
            F.col("l.protocol_no").alias("protocol_no"),
            F.col("r.contract_id").alias("contract_id"),
            F.col("r.contract_name").alias("contract_name"),
            F.col("r.sponsor_external_id").alias("sponsor_external_id"),
            "sponsor_match_score",
            "study_match_score",
            "contract_match_score",
            "overall_match_confidence",
            "match_level",
        )
    )

    gp_std = with_standardized_keys(
        great_plains,
        {
            "invoice_id": "invoice_id_std",
            "contract_name": "contract_name_std",
            "protocol_no": "protocol_no_std",
            "sponsor_external_id": "sponsor_external_id_std",
        },
    )

    return crosswalk.alias("c").join(
        gp_std.alias("g"),
        (F.col("c.sponsor_external_id") == F.col("g.sponsor_external_id"))
        & (
            (normalize_token("c.contract_name") == F.col("g.contract_name_std"))
            | (normalize_token("c.protocol_no") == F.col("g.protocol_no_std"))
        ),
        "left",
    ).select("c.*", F.col("g.invoice_id").alias("invoice_id"), F.col("g.invoice_number").alias("invoice_number"))


def build_entity_crosswalk(sponsor_crosswalk: DataFrame, study_contract_crosswalk: DataFrame) -> DataFrame:
    return study_contract_crosswalk.alias("sc").join(
        sponsor_crosswalk.alias("sp"),
        F.col("sc.sponsor_external_id") == F.col("sp.source_sponsor_external_id"),
        "left",
    ).select(
        F.col("sc.study_id"),
        F.col("sc.contract_id"),
        F.col("sc.invoice_id"),
        F.col("sc.sponsor_external_id"),
        F.col("sp.master_sponsor_external_id"),
        F.greatest(F.col("sc.overall_match_confidence"), F.col("sp.overall_match_confidence")).alias("overall_match_confidence"),
        F.col("sc.sponsor_match_score"),
        F.col("sc.study_match_score"),
        F.col("sc.contract_match_score"),
    )


def calculate_match_accuracy(scored_df: DataFrame, expected_col: str = "expected_match") -> float:
    """Compute match accuracy for labeled validation records."""
    evaluated = scored_df.withColumn("predicted_match", F.col("overall_match_confidence") >= 0.97)
    summary = evaluated.select(
        F.avg(F.when(F.col("predicted_match") == F.col(expected_col), 1).otherwise(0)).alias("accuracy")
    ).collect()[0]
    return float(summary["accuracy"] or 0.0)

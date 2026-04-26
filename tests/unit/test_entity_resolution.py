from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from clinical_lakehouse.transformations.entity_resolution import (
    build_study_contract_crosswalk,
    calculate_match_accuracy,
)


def test_entity_resolution_accuracy_target_on_mock_data():
    spark = SparkSession.builder.master("local[1]").appName("test_entity_resolution").getOrCreate()

    oncore = spark.read.option("header", True).csv("data/sample_source/oncore/batch_date=*/oncore.csv")
    relisource = spark.read.option("header", True).csv("data/sample_source/relisource/batch_date=*/relisource.csv")
    gp = spark.read.option("header", True).csv("data/sample_source/great_plains/batch_date=*/great_plains.csv")

    crosswalk = build_study_contract_crosswalk(oncore=oncore, relisource=relisource, great_plains=gp)

    validation_path = Path("data/sample_source/validation/entity_resolution_validation.csv")
    assert validation_path.exists()

    validation = spark.read.option("header", True).csv(str(validation_path)).withColumn(
        "expected_match", F.col("expected_match") == F.lit("TRUE")
    )

    scored = validation.join(
        crosswalk.select("study_id", "contract_id", "overall_match_confidence"),
        on=["study_id", "contract_id"],
        how="left",
    ).fillna({"overall_match_confidence": 0.0})

    accuracy = calculate_match_accuracy(scored, expected_col="expected_match")
    assert accuracy >= 0.97

    spark.stop()

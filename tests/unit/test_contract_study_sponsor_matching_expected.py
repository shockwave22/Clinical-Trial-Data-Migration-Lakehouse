import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.entity_resolution import build_study_contract_crosswalk


def test_contract_study_sponsor_matching_expected_output():
    expected = json.loads(Path("tests/expected/matching_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_matching").getOrCreate()

    oncore = spark.read.option("header", True).csv("data/sample_source/oncore/batch_date=*/oncore.csv")
    relisource = spark.read.option("header", True).csv("data/sample_source/relisource/batch_date=*/relisource.csv")
    gp = spark.read.option("header", True).csv("data/sample_source/great_plains/batch_date=*/great_plains.csv")

    crosswalk = build_study_contract_crosswalk(oncore=oncore, relisource=relisource, great_plains=gp)
    row = crosswalk.filter(crosswalk.study_id == "STUDY-2200").select("contract_id", "sponsor_external_id").first()

    assert row["contract_id"] == expected["expected_contract_id"]
    assert row["sponsor_external_id"] == expected["expected_sponsor_id"]
    spark.stop()

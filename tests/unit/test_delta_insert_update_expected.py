import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.delta_load_processing import detect_delta_changes


def test_delta_insert_update_detection_expected_output():
    expected = json.loads(Path("tests/expected/delta_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_delta_expected").getOrCreate()
    source = spark.createDataFrame([
        {"id": "A", "record_hash": "h1"},
        {"id": "B", "record_hash": "h2x"},
        {"id": "D", "record_hash": "h4"},
    ])
    target = spark.createDataFrame([
        {"id": "A", "record_hash": "h1"},
        {"id": "B", "record_hash": "h2"},
        {"id": "C", "record_hash": "h3"},
    ])

    out = detect_delta_changes(source, target, key_cols=["id"]).select("delta_business_key", "change_type").collect()
    actual = {r["delta_business_key"]: r["change_type"] for r in out}
    assert actual == expected
    spark.stop()

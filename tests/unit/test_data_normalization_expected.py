import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.entity_matching import normalize_join_keys


def test_data_normalization_against_expected_output():
    expected = json.loads(Path("tests/expected/normalization_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_normalization").getOrCreate()
    df = spark.createDataFrame([{"protocol_number": " p-1001 ", "sponsor_name": "Acme Bio!"}])
    out = normalize_join_keys(df, ["protocol_number", "sponsor_name"]).collect()[0]

    assert out["protocol_number"] == expected["protocol_no"]
    assert out["sponsor_name"] == expected["sponsor_name"]
    spark.stop()

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.utils.hash_utils import with_record_hash


def test_record_hash_generation_expected_length():
    expected = json.loads(Path("tests/expected/hash_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_hash").getOrCreate()
    df = spark.createDataFrame([{"a": "x", "b": "y"}])
    out = with_record_hash(df, ["a", "b"]).collect()[0]

    assert len(out["record_hash"]) == expected["hash_length"]
    spark.stop()

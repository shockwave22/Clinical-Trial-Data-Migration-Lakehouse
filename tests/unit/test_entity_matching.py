import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.entity_matching import normalize_join_keys


def test_normalize_join_keys_removes_special_characters():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([
        {"protocol_number": " p-1001 ", "sponsor_name": "Acme Bio!"}
    ])

    out = normalize_join_keys(df, ["protocol_number", "sponsor_name"]).collect()[0]

    assert out["protocol_number"] == "P1001"
    assert out["sponsor_name"] == "ACMEBIO"
    spark.stop()

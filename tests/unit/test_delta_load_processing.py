import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.delta_load_processing import (
    CHANGE_DELETE,
    CHANGE_INSERT,
    CHANGE_NO_CHANGE,
    CHANGE_UPDATE,
    detect_delta_changes,
)


def test_detect_delta_changes_classifies_insert_update_delete_nochange():
    spark = SparkSession.builder.master("local[1]").appName("test_delta_load").getOrCreate()

    source = spark.createDataFrame([
        {"id": "A", "record_hash": "h1"},   # no change
        {"id": "B", "record_hash": "h2x"},  # update
        {"id": "D", "record_hash": "h4"},   # insert
    ])
    target = spark.createDataFrame([
        {"id": "A", "record_hash": "h1"},
        {"id": "B", "record_hash": "h2"},
        {"id": "C", "record_hash": "h3"},   # delete
    ])

    delta = detect_delta_changes(source, target, key_cols=["id"])

    actual = {r["delta_business_key"]: r["change_type"] for r in delta.select("delta_business_key", "change_type").collect()}
    assert actual["A"] == CHANGE_NO_CHANGE
    assert actual["B"] == CHANGE_UPDATE
    assert actual["C"] == CHANGE_DELETE
    assert actual["D"] == CHANGE_INSERT

    spark.stop()

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.reconciliation.framework import (
    change_type_counts,
    duplicate_count,
    source_vs_target_record_count,
)


def test_reconciliation_framework_metrics_basic():
    spark = SparkSession.builder.master("local[1]").appName("test_recon_framework").getOrCreate()

    src = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 2}])
    tgt = spark.createDataFrame([{"id": 1}])

    rc = source_vs_target_record_count(src, tgt, "record_count")
    rows = {r["metric_name"]: r["metric_value"] for r in rc.collect()}
    assert rows["source_record_count"] == 3.0
    assert rows["target_record_count"] == 1.0

    dup = duplicate_count(src, ["id"]).collect()[0]["metric_value"]
    assert dup == 1.0

    delta = spark.createDataFrame(
        [
            {"change_type": "insert"},
            {"change_type": "update"},
            {"change_type": "delete"},
            {"change_type": "no_change"},
        ]
    )
    cc = {r["metric_name"]: r["metric_value"] for r in change_type_counts(delta).collect()}
    assert cc["insert_count"] == 1.0
    assert cc["update_count"] == 1.0
    assert cc["delete_count"] == 1.0
    assert cc["no_change_count"] == 1.0

    spark.stop()

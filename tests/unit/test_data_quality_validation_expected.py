import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.quality.data_quality import referential_integrity_orphans, required_field_violations


def test_data_quality_validation_expected_output():
    expected = json.loads(Path("tests/expected/data_quality_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_dq_expected").getOrCreate()
    study_df = spark.createDataFrame([
        {"protocol_no": "P1", "sponsor_external_id": "SPN1"},
        {"protocol_no": "", "sponsor_external_id": "SPN2"},
    ])
    parent_df = spark.createDataFrame([{"sponsor_external_id": "SPN1"}])

    violation_row = required_field_violations(study_df, ["protocol_no"]).collect()[0]
    orphans = referential_integrity_orphans(study_df, parent_df, "sponsor_external_id")

    assert violation_row["protocol_no"] == expected["protocol_violations"]
    assert orphans == expected["orphan_count"]
    spark.stop()

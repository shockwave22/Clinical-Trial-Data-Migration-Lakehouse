import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.ingestion.autoloader_ingest import add_ingestion_metadata


def test_add_ingestion_metadata_adds_required_columns():
    spark = SparkSession.builder.master("local[1]").appName("test_bronze_ingestion").getOrCreate()
    df = spark.createDataFrame([
        {
            "study_id": "STUDY-1001",
            "study_name": "LUNG-101",
            "protocol_no": "P-1001",
        }
    ])

    out = add_ingestion_metadata(df, source_system="oncore", load_id="test_load")

    expected_cols = {
        "study_id",
        "study_name",
        "protocol_no",
        "source_system",
        "source_file_name",
        "ingestion_timestamp",
        "load_id",
        "batch_date",
        "record_hash",
    }
    assert expected_cols.issubset(set(out.columns))
    record = out.collect()[0]
    assert record["source_system"] == "oncore"
    assert record["load_id"] == "test_load"
    assert record["record_hash"]
    spark.stop()

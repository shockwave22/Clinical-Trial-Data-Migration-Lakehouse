import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.transformations.silver_transformations import (
    transform_silver_contract,
    transform_silver_invoice,
    transform_silver_study,
)


def test_transform_silver_study_flags_missing_protocol():
    spark = SparkSession.builder.master("local[1]").appName("test_silver_study").getOrCreate()
    bronze = spark.createDataFrame([
        {
            "study_name": "Study A",
            "protocol_no": "",
            "sponsor_external_id": "SPN-001",
            "site_code": "NYC01",
            "start_date": "2026-01-01",
            "end_date": "2026-12-31",
        }
    ])

    valid, rejected = transform_silver_study(bronze)
    assert valid.count() == 0
    assert rejected.count() == 1
    assert "protocol_no_required" in rejected.collect()[0]["invalid_reason"]
    spark.stop()


def test_transform_silver_invoice_flags_bad_amounts():
    spark = SparkSession.builder.master("local[1]").appName("test_silver_invoice").getOrCreate()
    bronze = spark.createDataFrame([
        {
            "invoice_id": "INV-1",
            "invoice_number": "A-1",
            "contract_name": "Contract A",
            "protocol_no": "P-1",
            "sponsor_external_id": "SPN-1",
            "invoice_date": "2026-01-01",
            "invoice_amount": 100.0,
            "paid_amount": 150.0,
            "outstanding_amount": 10.0,
        }
    ])
    valid, rejected = transform_silver_invoice(bronze)
    assert valid.count() == 0
    reason = rejected.collect()[0]["invalid_reason"]
    assert "invoice_amount_lt_paid_amount" in reason
    assert "outstanding_mismatch" in reason
    spark.stop()


def test_transform_silver_contract_contract_value_positive_rule():
    spark = SparkSession.builder.master("local[1]").appName("test_silver_contract").getOrCreate()
    bronze = spark.createDataFrame([
        {
            "contract_name": "C1",
            "study_name": "S1",
            "protocol_no": "P1",
            "sponsor_external_id": "SPN1",
            "effective_date": "2026-01-01",
            "contract_value": -1.0,
        }
    ])
    valid, rejected = transform_silver_contract(bronze)
    assert valid.count() == 0
    assert "contract_value_must_be_positive" in rejected.collect()[0]["invalid_reason"]
    spark.stop()

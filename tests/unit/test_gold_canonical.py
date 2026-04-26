import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.exports.gold_netsuite_canonical import (
    build_gold_netsuite_invoice_header,
    build_gold_netsuite_open_ar,
)


def test_gold_open_ar_contains_positive_due_only():
    spark = SparkSession.builder.master("local[1]").appName("test_gold_open_ar").getOrCreate()

    silver_invoice = spark.createDataFrame([
        {
            "invoice_id": "INV-1",
            "invoice_number": "1001",
            "protocol_no": "P-1",
            "sponsor_external_id_std": "SPN1",
            "invoice_date_std": "2026-01-01",
            "payment_status": "Unpaid",
            "invoice_amount_std": 100.0,
            "paid_amount_std": 0.0,
            "outstanding_amount_std": 100.0,
        },
        {
            "invoice_id": "INV-2",
            "invoice_number": "1002",
            "protocol_no": "P-2",
            "sponsor_external_id_std": "SPN2",
            "invoice_date_std": "2026-01-01",
            "payment_status": "Paid",
            "invoice_amount_std": 50.0,
            "paid_amount_std": 50.0,
            "outstanding_amount_std": 0.0,
        },
    ])

    crosswalk = spark.createDataFrame([
        {"invoice_id": "INV-1", "study_id": "ST-1", "contract_id": "C-1", "protocol_no": "P-1"},
        {"invoice_id": "INV-2", "study_id": "ST-2", "contract_id": "C-2", "protocol_no": "P-2"},
    ])

    header = build_gold_netsuite_invoice_header(silver_invoice, crosswalk)
    open_ar = build_gold_netsuite_open_ar(header)

    assert open_ar.count() == 1
    assert open_ar.collect()[0]["invoice_external_id"] == "INV-1"
    spark.stop()

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from clinical_lakehouse.reconciliation.reporting import build_ar_reconciliation


def test_invoice_reconciliation_expected_output():
    expected = json.loads(Path("tests/expected/reconciliation_expected.json").read_text())

    spark = SparkSession.builder.master("local[1]").appName("test_reconciliation_expected").getOrCreate()
    invoices = spark.createDataFrame([{"sponsor_name": "Acme Bio", "study_name": "LUNG-101", "invoice_amount": 100.0}])
    payments = spark.createDataFrame([{"sponsor_name": "Acme Bio", "study_name": "LUNG-101", "payment_amount": 40.0}])

    out = build_ar_reconciliation(invoices, payments).collect()[0]
    assert out["outstanding_amount"] == expected["outstanding_amount"]
    spark.stop()

"""Generate mock clinical trial source files for local and Databricks testing."""

from __future__ import annotations

import csv
from pathlib import Path

BASE = Path("data/generated")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    studies = [
        {"study_name": "LUNG-101", "protocol_number": "P-1001", "sponsor_name": "Acme Bio", "site_code": "NYC01"},
        {"study_name": "CARDIO-220", "protocol_number": "P-2200", "sponsor_name": "Northwind Pharma", "site_code": "BOS02"},
    ]
    contracts = [
        {"contract_name": "ACME-LUNG-2026", "protocol_number": "P-1001", "sponsor_name": "Acme Bio", "external_id": "CNT-1"},
        {"contract_name": "NW-CARDIO-2026", "protocol_number": "P-2200", "sponsor_name": "Northwind Pharma", "external_id": "CNT-2"},
    ]
    invoices = [
        {"study_name": "LUNG-101", "sponsor_name": "Acme Bio", "invoice_amount": 120000},
        {"study_name": "CARDIO-220", "sponsor_name": "Northwind Pharma", "invoice_amount": 90000},
    ]
    payments = [
        {"study_name": "LUNG-101", "sponsor_name": "Acme Bio", "payment_amount": 70000},
        {"study_name": "CARDIO-220", "sponsor_name": "Northwind Pharma", "payment_amount": 50000},
    ]

    _write_csv(BASE / "oncore_studies.csv", studies)
    _write_csv(BASE / "reli_contracts.csv", contracts)
    _write_csv(BASE / "gp_invoices.csv", invoices)
    _write_csv(BASE / "gp_payments.csv", payments)


if __name__ == "__main__":
    main()

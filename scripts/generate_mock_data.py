"""Generate realistic mock clinical trial migration data with quality issues.

Output layout:
    data/sample_source/{system}/batch_date=YYYY-MM-DD/{system}.csv

Generates:
- 1 initial master load
- 3 incremental delta batches
"""

from __future__ import annotations

import csv
from copy import deepcopy
from pathlib import Path

BASE_DIR = Path("data/sample_source")
SYSTEMS = ("oncore", "relisource", "great_plains", "sponsor_master")
BATCH_DATES = ["2026-01-01", "2026-01-15", "2026-02-01", "2026-02-15"]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _oncore_master() -> list[dict]:
    return [
        {
            "study_id": "STUDY-1001",
            "study_name": "LUNG-101",
            "protocol_no": "P-1001",
            "sponsor_external_id": "SPN-001",
            "site_code": "NYC01",
            "principal_investigator": "Dr. Maria Chen",
            "therapeutic_area": "Oncology",
            "study_status": "Active",
            "start_date": "2025-01-10",
            "end_date": "2027-12-31",
        },
        {
            "study_id": "STUDY-2200",
            "study_name": "CARDIO-220",
            "protocol_no": "P-2200",
            "sponsor_external_id": "SPN-002",
            "site_code": "BOS02",
            "principal_investigator": "Dr. Ethan Brooks",
            "therapeutic_area": "Cardiology",
            "study_status": "Active",
            "start_date": "2024-11-01",
            "end_date": "2026-11-15",
        },
        {
            "study_id": "STUDY-3300",
            "study_name": "NEURO-330",
            "protocol_no": "P-3300",
            "sponsor_external_id": "SPN-003",
            "site_code": "SFO03",
            "principal_investigator": "Dr. Priya Raman",
            "therapeutic_area": "Neurology",
            "study_status": "Closed",
            "start_date": "2023-02-20",
            "end_date": "2025-12-15",
        },
    ]


def _relisource_master() -> list[dict]:
    return [
        {
            "contract_id": "CNT-9001",
            "contract_name": "ACME-LUNG-2026",
            "study_name": "LUNG-101",
            "protocol_no": "P-1001",
            "sponsor_external_id": "SPN-001",
            "site_code": "NYC01",
            "contract_status": "Active",
            "contract_value": 750000.0,
            "effective_date": "2025-01-01",
            "amendment_no": 0,
        },
        {
            "contract_id": "CNT-9002",
            "contract_name": "NW_CARDIO_2026",
            "study_name": "CARDIO-220",
            "protocol_no": "P-2200",
            "sponsor_external_id": "SPN-002",
            "site_code": "BOS02",
            "contract_status": "Active",
            "contract_value": 500000.0,
            "effective_date": "2024-11-01",
            "amendment_no": 1,
        },
        {
            "contract_id": "CNT-9003",
            "contract_name": "Global Trial Neuro 330",
            "study_name": "NEURO-330",
            "protocol_no": "P-3300",
            "sponsor_external_id": "SPN-003",
            "site_code": "SFO03",
            "contract_status": "Closed",
            "contract_value": 315000.0,
            "effective_date": "2023-02-01",
            "amendment_no": 2,
        },
    ]


def _great_plains_master() -> list[dict]:
    return [
        {
            "invoice_id": "INV-0001",
            "invoice_number": "GP-10001",
            "contract_name": "ACME LUNG 2026",  # formatting difference vs ReliSource
            "protocol_no": "P-1001",
            "sponsor_external_id": "SPN-001",
            "invoice_amount": 120000.0,
            "paid_amount": 60000.0,
            "outstanding_amount": 60000.0,
            "invoice_date": "2026-01-01",
            "payment_status": "Partially Paid",
        },
        {
            "invoice_id": "INV-0002",
            "invoice_number": "GP-10002",
            "contract_name": "NW-CARDIO-2026",  # formatting difference vs ReliSource
            "protocol_no": "P-2200",
            "sponsor_external_id": "SPN-002",
            "invoice_amount": 90000.0,
            "paid_amount": 90000.0,
            "outstanding_amount": 0.0,
            "invoice_date": "2026-01-03",
            "payment_status": "Paid",
        },
        {
            "invoice_id": "INV-0003",
            "invoice_number": "GP-10003",
            "contract_name": "Global Trial Neuro 330",
            "protocol_no": "P-3300",
            "sponsor_external_id": "SPN-003",
            "invoice_amount": 45000.0,
            "paid_amount": 10000.0,
            "outstanding_amount": 35000.0,  # closed study with open AR
            "invoice_date": "2025-12-20",
            "payment_status": "Partially Paid",
        },
    ]


def _sponsor_master() -> list[dict]:
    return [
        {
            "sponsor_external_id": "SPN-001",
            "sponsor_name": "Acme Biopharma",
            "sponsor_parent": "Acme Holdings",
            "billing_account_id": "BA-1001",
            "netsuite_customer_external_id": "NS-CUST-ACME",
            "sponsor_status": "Active",
        },
        {
            "sponsor_external_id": "SPN-002",
            "sponsor_name": "Northwind Pharmaceuticals",
            "sponsor_parent": "Northwind Group",
            "billing_account_id": "BA-2001",
            "netsuite_customer_external_id": "NS-CUST-NW",
            "sponsor_status": "Active",
        },
        {
            "sponsor_external_id": "SPN-003",
            "sponsor_name": "Global Trial Therapeutics",
            "sponsor_parent": "GTT Parent",
            "billing_account_id": "BA-3001",
            "netsuite_customer_external_id": "NS-CUST-GTT",
            "sponsor_status": "Active",
        },
    ]


def _batch_payloads() -> list[dict[str, list[dict]]]:
    """Return initial batch + 3 incremental deltas with intentional DQ issues."""
    batch_1 = {
        "oncore": _oncore_master(),
        "relisource": _relisource_master(),
        "great_plains": _great_plains_master(),
        "sponsor_master": _sponsor_master(),
    }

    batch_2 = {
        "oncore": [
            {
                "study_id": "STUDY-4400",
                "study_name": "IMMUNO-440",
                "protocol_no": "",  # missing protocol number
                "sponsor_external_id": "SPN-001",
                "site_code": "CHI04",
                "principal_investigator": "Dr. Lily Tran",
                "therapeutic_area": "Immunology",
                "study_status": "Planned",
                "start_date": "2026-03-01",
                "end_date": "2028-12-01",
            }
        ],
        "relisource": [
            {
                "contract_id": "CNT-9004",
                "contract_name": "ACME_LUNG_2026",  # formatting variation
                "study_name": "LUNG-101",
                "protocol_no": "P-1001",
                "sponsor_external_id": "SPN-001",
                "site_code": "NYC01",
                "contract_status": "Active",
                "contract_value": 785000.0,  # updated contract value
                "effective_date": "2026-01-15",
                "amendment_no": 1,
            }
        ],
        "great_plains": [
            {
                "invoice_id": "INV-0004",
                "invoice_number": "GP-10004",
                "contract_name": "ACME-LUNG-2026",
                "protocol_no": "P-1001",
                "sponsor_external_id": "SPN-001",
                "invoice_amount": 50000.0,
                "paid_amount": 0.0,
                "outstanding_amount": 50000.0,
                "invoice_date": "2026-01-14",  # late arriving record in later batch
                "payment_status": "Unpaid",
            },
            {
                "invoice_id": "INV-0004",  # duplicate invoice
                "invoice_number": "GP-10004",
                "contract_name": "ACME-LUNG-2026",
                "protocol_no": "P-1001",
                "sponsor_external_id": "SPN-001",
                "invoice_amount": 50000.0,
                "paid_amount": 0.0,
                "outstanding_amount": 50000.0,
                "invoice_date": "2026-01-14",
                "payment_status": "Unpaid",
            },
        ],
        "sponsor_master": [
            {
                "sponsor_external_id": "SPN-001",
                "sponsor_name": "ACME Biopharma",  # variation in casing
                "sponsor_parent": "Acme Holdings",
                "billing_account_id": "BA-1001",
                "netsuite_customer_external_id": "NS-CUST-ACME",
                "sponsor_status": "Active",
            }
        ],
    }

    batch_3 = {
        "oncore": [
            {
                "study_id": "STUDY-3300",
                "study_name": "NEURO-330",
                "protocol_no": "P-3300",
                "sponsor_external_id": "SPN-003",
                "site_code": "SFO03",
                "principal_investigator": "Dr. Priya Raman",
                "therapeutic_area": "Neurology",
                "study_status": "Closed",
                "start_date": "2023-02-20",
                "end_date": "2025-12-15",
            }
        ],
        "relisource": [
            {
                "contract_id": "CNT-9002",
                "contract_name": "NW-CARDIO-2026",  # formatting standardized update
                "study_name": "CARDIO-220",
                "protocol_no": "P-2200",
                "sponsor_external_id": "SPN-002",
                "site_code": "BOS02",
                "contract_status": "Active",
                "contract_value": 540000.0,  # updated contract values
                "effective_date": "2026-02-01",
                "amendment_no": 2,
            }
        ],
        "great_plains": [
            {
                "invoice_id": "INV-0005",
                "invoice_number": "GP-10005",
                "contract_name": "Global Trial Neuro 330",
                "protocol_no": "P-3300",
                "sponsor_external_id": "SPN-003",
                "invoice_amount": 25000.0,
                "paid_amount": 0.0,
                "outstanding_amount": 25000.0,
                "invoice_date": "2026-01-31",  # late arriving in February batch
                "payment_status": "Unpaid",
            }
        ],
        "sponsor_master": [
            {
                "sponsor_external_id": "SPN-002",
                "sponsor_name": "Northwind Pharma",  # sponsor name variation
                "sponsor_parent": "Northwind Group",
                "billing_account_id": "BA-2001",
                "netsuite_customer_external_id": "NS-CUST-NW",
                "sponsor_status": "Active",
            }
        ],
    }

    batch_4 = {
        "oncore": [
            {
                "study_id": "STUDY-5500",
                "study_name": "RARE-550",
                "protocol_no": "P-5500",
                "sponsor_external_id": "SPN-004",
                "site_code": "DAL05",
                "principal_investigator": "Dr. Samuel Ortega",
                "therapeutic_area": "Rare Disease",
                "study_status": "Active",
                "start_date": "2026-02-14",
                "end_date": "2029-03-31",
            }
        ],
        "relisource": [
            {
                "contract_id": "CNT-9010",
                "contract_name": "RARE550|MASTER",  # formatting variation
                "study_name": "RARE-550",
                "protocol_no": "P-5500",
                "sponsor_external_id": "SPN-004",
                "site_code": "DAL05",
                "contract_status": "Active",
                "contract_value": 910000.0,
                "effective_date": "2026-02-15",
                "amendment_no": 0,
            }
        ],
        "great_plains": [
            {
                "invoice_id": "INV-0006",
                "invoice_number": "GP-10006",
                "contract_name": "RARE 550 MASTER",  # formatting difference
                "protocol_no": "",  # missing protocol number
                "sponsor_external_id": "SPN-004",
                "invoice_amount": 15000.0,
                "paid_amount": 0.0,
                "outstanding_amount": 15000.0,
                "invoice_date": "2026-02-15",
                "payment_status": "Unpaid",
            }
        ],
        "sponsor_master": [
            {
                "sponsor_external_id": "SPN-004",
                "sponsor_name": "RareCure Biotech",
                "sponsor_parent": "RareCure Group",
                "billing_account_id": "BA-4001",
                "netsuite_customer_external_id": "NS-CUST-RC",
                "sponsor_status": "Pending",
            }
        ],
    }

    return [batch_1, batch_2, batch_3, batch_4]


def _write_batch(batch_date: str, payload: dict[str, list[dict]]) -> None:
    for system, rows in payload.items():
        output_file = BASE_DIR / system / f"batch_date={batch_date}" / f"{system}.csv"
        _write_csv(output_file, deepcopy(rows))


def main() -> None:
    batches = _batch_payloads()
    for date_str, payload in zip(BATCH_DATES, batches, strict=True):
        _write_batch(date_str, payload)


if __name__ == "__main__":
    main()

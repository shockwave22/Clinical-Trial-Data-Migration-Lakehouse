import csv
from pathlib import Path

from scripts.generate_mock_data import main


SYSTEMS = ["oncore", "relisource", "great_plains", "sponsor_master"]
BATCHES = ["2026-01-01", "2026-01-15", "2026-02-01", "2026-02-15"]


def _load_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_mock_data_generator_creates_batches_and_system_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    main()

    for system in SYSTEMS:
        for batch_date in BATCHES:
            expected = Path(f"data/sample_source/{system}/batch_date={batch_date}/{system}.csv")
            assert expected.exists(), f"missing: {expected}"


def test_mock_data_generator_intentional_data_quality_issues_present(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    main()

    # missing protocol number issue
    oncore_delta = _load_rows(Path("data/sample_source/oncore/batch_date=2026-01-15/oncore.csv"))
    assert any(not row["protocol_no"] for row in oncore_delta)

    # duplicate invoices issue
    gp_delta = _load_rows(Path("data/sample_source/great_plains/batch_date=2026-01-15/great_plains.csv"))
    invoice_ids = [row["invoice_id"] for row in gp_delta]
    assert len(invoice_ids) != len(set(invoice_ids))

    # sponsor name variation issue
    sponsor_delta = _load_rows(Path("data/sample_source/sponsor_master/batch_date=2026-02-01/sponsor_master.csv"))
    assert sponsor_delta[0]["sponsor_name"] == "Northwind Pharma"

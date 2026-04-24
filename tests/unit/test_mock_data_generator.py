from pathlib import Path

from scripts.generate_mock_data import main


def test_mock_data_generator_creates_expected_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    main()

    assert Path("data/generated/oncore_studies.csv").exists()
    assert Path("data/generated/reli_contracts.csv").exists()
    assert Path("data/generated/gp_invoices.csv").exists()
    assert Path("data/generated/gp_payments.csv").exists()

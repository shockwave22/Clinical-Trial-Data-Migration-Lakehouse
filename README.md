# Clinical Trial Data Migration Lakehouse (Databricks Portfolio Project)

A production-style **Databricks + Delta Lake** project that demonstrates how clinical trial operational and financial data can be migrated from multiple source systems and conformed into **NetSuite-ready target tables**.

## Why this project matters (plain-English business story)
Clinical trials are managed across many tools that do not naturally agree with each other:
- **OnCore CTMS** tracks studies, protocols, sites, and investigators.
- **ReliSource Contracts** tracks contracts, amendments, and milestones.
- **Great Plains** tracks invoices, payments, and outstanding accounts receivable.
- **Sponsor Master** tracks sponsor hierarchy and billing accounts.

Finance and operations teams need a single trusted view to answer questions like:
- Which sponsor owes us money for which study?
- Are invoices aligned to the latest contract amendment?
- Which site activity has not yet been billed?

This project builds a medallion pipeline (Bronze/Silver/Gold) to standardize and link records using critical keys:
`Contract Name`, `Study Name`, `Protocol Number`, `External IDs`, `Sponsor Name`, and `Site Code`.

## Architecture at a glance
- **Bronze**: raw ingestion via Auto Loader-style logic into Delta tables.
- **Silver**: standardized, deduplicated, incrementally merged entities.
- **Gold**: business-ready marts for reconciliation and NetSuite export.
- **Incremental logic**: record hash + Delta `MERGE` upsert patterns.
- **Entity matching**: fuzzy-safe key normalization and deterministic fallback joins.
- **Data quality**: required field checks, referential checks, and threshold gates.
- **Reconciliation**: billed vs collected vs outstanding metrics.

Detailed architecture and diagrams:
- [`docs/architecture/solution_architecture.md`](docs/architecture/solution_architecture.md)
- [`architecture/architecture_diagram.mmd`](architecture/architecture_diagram.mmd)

## Repository structure
```text
.
├── architecture/                 # diagram source files
├── data/
│   ├── raw/                      # source landing data by system
│   ├── generated/                # synthetic datasets
│   └── exports/                  # NetSuite-ready output extracts
├── docs/
│   ├── architecture/             # architecture and design docs
│   ├── mappings/                 # source-to-target field mapping
│   └── runbooks/                 # operational runbooks
├── notebooks/                    # Databricks notebook-style Python scripts
├── scripts/                      # helper scripts (mock data generation)
├── src/clinical_lakehouse/       # PySpark package
└── tests/                        # pytest unit tests
```

## Quick start
1. Generate mock source data:
   ```bash
   python scripts/generate_mock_data.py
   ```
2. Run unit tests:
   ```bash
   pytest -q
   ```
3. Import notebook scripts into Databricks workspace and execute in order:
   - `notebooks/01_bronze_ingestion.py`
   - `notebooks/02_silver_conformance.py`
   - `notebooks/03_gold_netsuite_export.py`

## Unity Catalog-ready naming convention
Catalog/schema/table pattern used by config:
- `clinical_migration_dev.bronze.oncore_study_raw`
- `clinical_migration_dev.bronze.relisource_contract_raw`
- `clinical_migration_dev.bronze.gp_invoice_raw`
- `clinical_migration_dev.bronze.sponsor_master_raw`

See [`src/clinical_lakehouse/config/settings.py`](src/clinical_lakehouse/config/settings.py).


## Silver transformation outputs
Standardized Silver tables produced by notebook `02_silver_conformance.py`:
- `clinical_migration_dev.silver.silver_study`
- `clinical_migration_dev.silver.silver_contract`
- `clinical_migration_dev.silver.silver_invoice`
- `clinical_migration_dev.silver.silver_sponsor`
- `clinical_migration_dev.silver.silver_site`

Rejected rows are written to quarantine tables under `clinical_migration_dev.silver_quarantine.*` with `invalid_reason`.

## Resume-ready impact highlights
- Built a **4-source clinical data migration** reference architecture using Databricks medallion design.
- Implemented **incremental Delta MERGE framework** using hash-based change detection.
- Delivered **entity resolution + reconciliation layer** to improve finance traceability.
- Produced **NetSuite-ready gold exports** and auditable data quality controls.

See full metrics in [`docs/runbooks/resume_project_summary.md`](docs/runbooks/resume_project_summary.md).

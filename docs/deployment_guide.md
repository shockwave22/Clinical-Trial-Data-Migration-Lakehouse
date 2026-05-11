# Deployment Guide

## 1) Run locally with PySpark
1. Create/activate a Python virtual environment.
2. Install dependencies (at minimum: `pyspark`, `pytest`; optionally `delta-spark`).
3. Generate mock data:
   ```bash
   python scripts/generate_mock_data.py
   ```
4. Run tests:
   ```bash
   PYTHONPATH=src:. pytest -q
   ```
5. Execute notebooks as scripts in order for local smoke testing:
   - `notebooks/01_bronze_ingestion.py`
   - `notebooks/02_silver_conformance.py`
   - `notebooks/03_gold_netsuite_export.py`
   - `notebooks/04_entity_resolution.py`
   - `notebooks/06_delta_load_processing.ipynb`
   - `notebooks/07_reconciliation_dashboard.ipynb`

## 2) Run in Databricks notebooks
1. Import this repository into Databricks Repos.
2. Configure Unity Catalog schemas (`bronze`, `silver`, `gold`, and `silver_quarantine`).
3. Attach a cluster/runtime with Delta support.
4. Run notebooks in sequence listed above.

## 3) Deploy with Databricks Asset Bundles
Deployment assets:
- `resources/databricks.yml`
- `resources/jobs.yml`
- `resources/permissions.yml`
- `resources/config/dev.yml`, `resources/config/test.yml`, `resources/config/prod.yml`
- `resources/env/.env.example`

### Example commands
```bash
# Authenticate (recommended with env vars)
export DATABRICKS_HOST=... 
export DATABRICKS_TOKEN=...
export DATABRICKS_CLUSTER_ID=...

# Validate bundle
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev

# Run workflow
databricks bundle run clinical_migration_pipeline -t dev
```

Use `-t test` or `-t prod` for higher environments.

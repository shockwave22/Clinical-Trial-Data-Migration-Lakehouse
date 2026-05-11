# Reconciliation Framework

This framework provides migration control metrics between source and target layers and is implemented in:
- `src/clinical_lakehouse/reconciliation/framework.py`
- `notebooks/07_reconciliation_dashboard.ipynb`

## Metrics covered
1. Source record count vs target record count
2. Total contract value by source vs target
3. Invoice total by source vs target
4. Open AR total by source vs target
5. Rejected record count
6. Duplicate count
7. Match accuracy
8. Load runtime
9. Insert/update/delete/no-change counts

## Inputs
- Source: `clinical_migration_dev.bronze.relisource_contract_raw`, `clinical_migration_dev.bronze.gp_invoice_raw`
- Target: `clinical_migration_dev.gold.gold_netsuite_contract_master`, `clinical_migration_dev.gold.gold_netsuite_invoice_header`, `clinical_migration_dev.gold.gold_netsuite_open_ar`
- Quality: `clinical_migration_dev.silver_quarantine.*`
- Entity resolution: `clinical_migration_dev.silver.silver_entity_crosswalk`
- Delta change audit: `clinical_migration_dev.audit.latest_delta_changes`

## Output
- Delta table: `clinical_migration_dev.gold.gold_reconciliation_dashboard`
- Columns:
  - `metric_group`
  - `metric_name`
  - `metric_value`
  - `report_generated_ts`

## Operational usage
- Run after Silver/Gold loads and delta processing notebook.
- Use as dashboard source for migration cutover readiness checks.
- Alert when deltas exceed thresholds (count delta, amount delta, low match accuracy).

# Source to Target Mapping: Gold NetSuite Canonical Tables

## Canonical Gold Tables
1. `gold_netsuite_customer_master`
2. `gold_netsuite_project_master`
3. `gold_netsuite_contract_master`
4. `gold_netsuite_invoice_header`
5. `gold_netsuite_invoice_line`
6. `gold_netsuite_open_ar`
7. `gold_migration_audit_summary`

## Mapping summary
| Source Domain | Source Table | Key Source Fields | Gold Target Table | Key Target Fields | Transformation Notes |
|---|---|---|---|---|---|
| Sponsor | bronze.sponsor_master_raw + silver.silver_sponsor_crosswalk | sponsor_external_id, sponsor_name, billing_account_id, netsuite_customer_external_id | gold_netsuite_customer_master | customer_external_id, customer_name, billing_account_id, netsuite_customer_external_id | Sponsor mapped to NetSuite Customer using sponsor crosswalk and confidence score |
| Study/Protocol | silver.silver_study | study_id, study_name_std, protocol_no_std, sponsor_external_id_std, site_code_std | gold_netsuite_project_master | project_external_id, project_name, protocol_no, customer_external_id, primary_site_code | Study/protocol standardized to NetSuite Project master |
| Contracts | silver.silver_contract + silver.silver_study_contract_crosswalk | contract_id, contract_name_std, contract_value_std, effective_date_std, amendment_no | gold_netsuite_contract_master | contract_external_id, contract_name, contract_value, effective_date, project_external_id | ReliSource contracts mapped to NetSuite Contract and linked to project |
| Invoices | silver.silver_invoice + silver.silver_study_contract_crosswalk | invoice_id, invoice_number, invoice_amount_std, paid_amount_std, outstanding_amount_std, invoice_date_std | gold_netsuite_invoice_header | invoice_external_id, invoice_number, invoice_total, amount_paid, amount_due, contract_external_id | Great Plains invoice mapped to NetSuite invoice header |
| Invoice Lines | gold_netsuite_invoice_header | invoice_external_id, project_external_id, contract_external_id, invoice_total | gold_netsuite_invoice_line | invoice_external_id, line_number, project_external_id, line_amount | Single-line canonical representation for portfolio/demo use |
| Open AR | gold_netsuite_invoice_header | amount_due, payment_status | gold_netsuite_open_ar | invoice_external_id, amount_due, payment_status | Filter invoices where amount_due > 0 |
| Migration Audit | all gold tables | row counts and sums | gold_migration_audit_summary | table_name, record_count, amount_total, audit_timestamp | Migration readiness and reconciliation audit controls |

## Business mapping requested
- Sponsor → NetSuite Customer
- Study/Protocol → NetSuite Project
- ReliSource Contract → NetSuite Contract
- Great Plains Invoice → NetSuite Invoice
- Outstanding AR → NetSuite Open AR

# Source-to-Target Mapping (Clinical Trial Migration)

## Core linking keys
- Contract Name
- Study Name
- Protocol Number
- External IDs
- Sponsor Name
- Site Code

## Mapping matrix
| Source System | Source Entity | Source Field | Target Table | Target Field | Notes |
|---|---|---|---|---|---|
| OnCore CTMS | studies | study_name | gold.study_dimension | study_name | canonical study label |
| OnCore CTMS | protocols | protocol_number | gold.study_dimension | protocol_number | primary enterprise key |
| OnCore CTMS | sites | site_code | gold.site_dimension | site_code | site rollup key |
| OnCore CTMS | investigators | investigator_external_id | gold.investigator_dimension | external_id | cross-system person key |
| ReliSource Contracts | contracts | contract_name | gold.contract_fact | contract_name | contractual header |
| ReliSource Contracts | contracts | sponsor_name | gold.contract_fact | sponsor_name | normalized for matching |
| ReliSource Contracts | amendments | amendment_version | gold.contract_fact | amendment_version | latest version selection |
| ReliSource Contracts | milestones | milestone_date | gold.contract_milestone_fact | milestone_date | billing trigger |
| Great Plains | invoices | invoice_amount | gold.ar_reconciliation | billed_amount | financial billed amount |
| Great Plains | payments | payment_amount | gold.ar_reconciliation | paid_amount | collections amount |
| Great Plains | outstanding AR | outstanding_amount | gold.ar_reconciliation | outstanding_amount | open AR position |
| Sponsor Master | sponsor hierarchy | parent_sponsor | gold.sponsor_dimension | parent_sponsor | hierarchy roll-up |
| Sponsor Master | billing accounts | billing_account_id | gold.netsuite_invoice_export | customer_external_id | NetSuite customer mapping |

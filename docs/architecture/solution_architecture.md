# Solution Architecture

## Design principles
1. Incremental-first ingestion with Auto Loader semantics.
2. Delta Lake ACID guarantees at each medallion layer.
3. Deterministic key normalization for entity resolution.
4. Data quality controls as pipeline gates.
5. Gold outputs aligned to NetSuite import expectations.

## Medallion data flow
- **Bronze**: Raw append-only ingestion from each source feed.
- **Silver**: Standardized schema, deduplication, type casting, and entity link table creation.
- **Gold**: Reconciliation marts and NetSuite-ready export tables.

## Pipeline stages
1. Land files from source extracts (CSV/JSON) into raw storage paths.
2. Ingest with Auto Loader-style reader + schema evolution handling.
3. Add `record_hash` and merge into Delta Silver tables.
4. Resolve entities by protocol/sponsor/site keys.
5. Run quality checks (required fields + referential integrity).
6. Publish reconciliation and export tables in Gold.

## Operational controls
- Checkpointing for streaming ingestion.
- Merge idempotency through key + hash logic.
- Audit columns (`ingest_ts`, `source_system`, `batch_id`) recommended on all core tables.
- Daily reconciliation report delivered to finance stakeholders.

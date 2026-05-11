"""Project configuration and naming helpers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "clinical_migration_dev"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def table_name(self, layer: str, entity: str) -> str:
        return f"{self.catalog}.{layer}.{entity}"


BRONZE_RAW_TABLES = {
    "oncore": "clinical_migration_dev.bronze.oncore_study_raw",
    "relisource": "clinical_migration_dev.bronze.relisource_contract_raw",
    "great_plains": "clinical_migration_dev.bronze.gp_invoice_raw",
    "sponsor_master": "clinical_migration_dev.bronze.sponsor_master_raw",
}

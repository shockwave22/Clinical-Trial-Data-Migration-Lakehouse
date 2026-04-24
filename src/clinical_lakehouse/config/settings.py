"""Project configuration and naming helpers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "ct_lakehouse_dev"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def table_name(self, layer: str, entity: str) -> str:
        return f"{self.catalog}.{layer}.{entity}"


SOURCE_TABLE_MAP = {
    "oncore_studies": ("bronze", "oncore_studies"),
    "oncore_protocols": ("bronze", "oncore_protocols"),
    "oncore_sites": ("bronze", "oncore_sites"),
    "oncore_investigators": ("bronze", "oncore_investigators"),
    "reli_contracts": ("bronze", "reli_contracts"),
    "reli_amendments": ("bronze", "reli_amendments"),
    "reli_milestones": ("bronze", "reli_milestones"),
    "gp_invoices": ("bronze", "gp_invoices"),
    "gp_payments": ("bronze", "gp_payments"),
    "gp_outstanding_ar": ("bronze", "gp_outstanding_ar"),
    "sponsor_master": ("bronze", "sponsor_master"),
}

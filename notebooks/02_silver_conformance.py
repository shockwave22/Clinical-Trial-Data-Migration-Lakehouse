# Databricks notebook source
"""Silver conformance notebook for entity standardization + matching."""

from clinical_lakehouse.transformations.entity_matching import build_study_contract_link
from clinical_lakehouse.transformations.incremental_merge import merge_on_keys
from clinical_lakehouse.utils.hash_utils import with_record_hash

# COMMAND ----------
# linked = build_study_contract_link(studies_df, contracts_df)
# hashed = with_record_hash(linked, linked.columns)
# merge_on_keys("ct_lakehouse_dev.silver.study_contract_link", hashed, "t.protocol_number = s.protocol_number")

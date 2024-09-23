# Databricks notebook source
import json

# COMMAND ----------

sat_vars = {
    "verbosity": "info",
    "warehouse_id": "",
    "catalog_name": "sat_david",
    "schema_name": "sat",
    "proxies": {},
    "account_url": "https://accounts.cloud.databricks.com",
    "workspace_baseUrl": ".cloud.databricks.com",
    "account_id": "0d26daa6-5e44-4c97-a497-ef015f91254a",
    "client_id": "34449fd8-e99d-4e9b-934c-0aa99d9e08b1",
    "client_secret": "",
}
sat_vars["database"] = f"{sat_vars['catalog_name']}.{sat_vars['schema_name']}"

# COMMAND ----------

# MAGIC %md
# MAGIC

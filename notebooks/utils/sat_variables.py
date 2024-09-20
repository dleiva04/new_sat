# Databricks notebook source
import json

# COMMAND ----------

sat_vars = {
    "verbosity": "info",
    "warehouse_id": "",
    "catalog_name": "new_sat_david",
    "schema_name": "sat",
    "proxies": {},
    "client_id": "",
    "client_secret": "",
}
sat_vars["database"] = f"{sat_vars['catalog_name']}.{sat_vars['schema_name']}"

# COMMAND ----------

# MAGIC %md
# MAGIC

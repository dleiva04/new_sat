# Databricks notebook source
from databricks.sdk import WorkspaceClient, AccountClient

# COMMAND ----------

# MAGIC %run "../utils/sat_variables"

# COMMAND ----------

account = AccountClient(
    host=sat_vars["account_url"],
    account_id=sat_vars["account_id"],
    client_id=sat_vars["client_id"],
    client_secret=sat_vars["client_secret"],
)

# COMMAND ----------

for w in account.workspaces.list():
    if w.workspace_status_message == 'Workspace is running.':
        try:
            workspace = WorkspaceClient(
                host=f'{w.deployment_name}{sat_vars["workspace_baseUrl"]}',
                client_id=sat_vars["client_id"],
                client_secret=sat_vars["client_secret"],
            )
            workspace.get_workspace_id()
            print(w.workspace_name)
            print("Valid")
        except Exception as e:
            continue

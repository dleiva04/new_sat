# Databricks notebook source
def basePath():
    path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    path = path[: path.find("/notebooks")]
    return f"/Workspace{path}"

# COMMAND ----------

basePath()

# COMMAND ----------

def getConfigsPath():
    return f"{basePath()}/notebooks/configs"

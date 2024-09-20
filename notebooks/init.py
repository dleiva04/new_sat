# Databricks notebook source
# MAGIC %run "./utils/sat_variables"

# COMMAND ----------

# MAGIC %run "./utils/sat_functions"

# COMMAND ----------

# create_schema
spark.sql(f'create database if not exists {sat_vars["database"]}')
spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {sat_vars["database"]}.run_number_table (
                        runID BIGINT GENERATED ALWAYS AS IDENTITY,
                        check_time TIMESTAMP 
                        )
                        USING DELTA"""
    )

# COMMAND ----------

# checks table
spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {sat_vars["database"]}.security_checks ( 
                workspaceid string,
                id int,
                score integer, 
                additional_details map<string, string>,
                run_id bigint,
                check_time timestamp,
                chk_date date GENERATED ALWAYS AS (CAST(check_time AS DATE)),
                chk_hhmm integer GENERATED ALWAYS AS (CAST(CAST(hour(check_time) as STRING) || CAST(minute(check_time) as STRING) as INTEGER))
                )
                USING DELTA
                PARTITIONED BY (chk_date)"""
    )

# COMMAND ----------

spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {sat_vars["database"]}.account_info (
        workspaceid string,
        name string, 
        value map<string, string>, 
        category string,
        run_id bigint,
        check_time timestamp,
        chk_date date GENERATED ALWAYS AS (CAST(check_time AS DATE)),\
        chk_hhmm integer GENERATED ALWAYS AS (CAST(CAST(hour(check_time) as STRING) || CAST(minute(check_time) as STRING) as INTEGER))
        )
        USING DELTA
        PARTITIONED BY (chk_date)"""
    )

# COMMAND ----------

spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {sat_vars["database"]}.account_workspaces (
            workspace_id string,
            deployment_url string,
            workspace_name string,
            workspace_status string,
            ws_token string,
            analysis_enabled boolean,
            sso_enabled boolean,
            scim_enabled boolean,
            vpc_peering_done boolean,
            object_storage_encrypted boolean,
            table_access_control_enabled boolean
            )
            USING DELTA"""
    )

# COMMAND ----------

spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {sat_vars["database"]}.workspace_run_complete(
                    workspace_id string,
                    run_id bigint,
                    completed boolean,
                    check_time timestamp,
                    chk_date date GENERATED ALWAYS AS (CAST(check_time AS DATE))
                    )
                    USING DELTA"""
    )

# COMMAND ----------

# read best practices config file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("check_id", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("check", StringType(), nullable=True),
    StructField("evaluation_value", IntegerType(), nullable=True),
    StructField("severity", StringType(), nullable=True),
    StructField("recommendation", StringType(), nullable=True),
    StructField("aws", IntegerType(), nullable=True),
    StructField("azure", IntegerType(), nullable=True),
    StructField("gcp", IntegerType(), nullable=True),
    StructField("enable", IntegerType(), nullable=True),
    StructField("alert", IntegerType(), nullable=True),
    StructField("logic", StringType(), nullable=True),
    StructField("api", StringType(), nullable=True),
    StructField("aws_doc_url", StringType(), nullable=True),
    StructField("azure_doc_url", StringType(), nullable=True),
    StructField("gcp_doc_url", StringType(), nullable=True)
])

if not spark.catalog.tableExists(f'{sat_vars["database"]}.security_best_practices'): 
    print(getConfigsPath())   
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"file:{getConfigsPath()}/security_best_practices.csv")
    )
    df = df.drop("azure_doc_url", "gcp_doc_url")
    df = df.withColumnRenamed("aws_doc_url", "doc_url")
    df.write.mode("overwrite").saveAsTable(f'{sat_vars["database"]}.security_best_practices')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType(
    [
        StructField("sat_id", IntegerType(), nullable=True),
        StructField("dasf_control_id", StringType(), nullable=True),
        StructField("dasf_control_name", StringType(), nullable=True),
    ]
)

sat_dasf_mapping = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(f"file:{getConfigsPath()}/sat_dasf_mapping.csv")
    .select("sat_id", "dasf_control_id", "dasf_control_name")
)

sat_dasf_mapping.write.format("delta").mode("overwrite").saveAsTable(
    f'{sat_vars["database"]}.sat_dasf_mapping'
)

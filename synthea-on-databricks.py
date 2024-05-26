# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "synthea")
dbutils.widgets.text("instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Node Type Id, Required if Instance Pool Id is not specified")
dbutils.widgets.text("number_of_job_runs", "1", "Number of times to run the job")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
instance_pool_id = dbutils.widgets.get("instance_pool_id")
node_type_id = dbutils.widgets.get("node_type_id")
number_of_job_runs = int(dbutils.widgets.get("number_of_job_runs"))

# COMMAND ----------

print(
f"""
Based on user input's the job will write files into this catalog.schema's Volume:
catalog_name = {catalog_name}
schema_name = {schema_name}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook. 

Cluster Specification Details: 
instance_pool_id = {instance_pool_id}
node_type_id = {node_type_id}

Note that node_type_id will only be used if an instance_pool_id is not set.

Number of times the Databricks workflow will be executed to simulate variability in patient record creation: number_of_job_runs = {number_of_job_runs}
"""
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

post_job_result = dbutils.notebook.run(
  path = "workflows/synthea-on-dbx-create-workflow"
  ,timeout_seconds = 150
  ,arguments = {
    "catalog_name": catalog_name
    ,"schema_name": schema_name
    ,"instance_pool_id": instance_pool_id
    ,"node_type_id": node_type_id
  }
)

# COMMAND ----------

import json

# COMMAND ----------

if json.loads(post_job_result)["status"] == "OK":
  job_id = json.loads(post_job_result)["job"]["job_id"]
  print(f""" 
    Databricks Workflow Creation Successful, use job id: {job_id}.
  """)
else:
  raise Exception(f"""Databricks Workflow Creation Failed, please check the job run output for more information.""")

# COMMAND ----------

help(w.jobs.run_now_and_wait)

# COMMAND ----------

from random import randint

# COMMAND ----------

wait_time = randint(300, 600)
wait_time

# COMMAND ----------

from time import sleep

# COMMAND ----------

for i in range(0, number_of_job_runs):
  if i == 0:
    w.jobs.run_now_and_wait(
      job_id = job_id
      ,job_parameters = {
        "catalog_name": catalog_name
        ,"schema_name": schema_name
      } 
    )
  else:
    sleep(wait_time)
    w.jobs.run_now_and_wait(
      job_id = job_id
      ,job_parameters = {
        "catalog_name": catalog_name
        ,"schema_name": schema_name
      } 
    )

# COMMAND ----------

runs = w.jobs.list_runs(job_id=job_id)
runs = [run.as_dict() for run in runs]

# COMMAND ----------

import pandas as pd

# COMMAND ----------

#create a pandas dataframe and then convert it to a pySpark dataframe
runs_pandas = pd.DataFrame(runs)
runs_df = spark.createDataFrame(runs_pandas)

# COMMAND ----------

display(runs_df)

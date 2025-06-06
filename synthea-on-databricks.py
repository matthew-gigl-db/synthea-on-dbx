# Databricks notebook source
# DBTITLE 1,Check Java Version of Serverless
# MAGIC %sh
# MAGIC java -version

# COMMAND ----------

# DBTITLE 1,install databricks sdk upgrade
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,get databricks-sdk version
# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

# DBTITLE 1,Databricks Workspace Client
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Determine Smallest Available Memory Optimized x86 Node Type Available
nodes = w.clusters.list_node_types()
nodes_list = [node.as_dict() for node in nodes.node_types]
nodes_df = spark.createDataFrame(nodes_list)
# display(nodes_df)
node_type_id = (
  nodes_df
  .filter(col("is_deprecated") == False)
  .filter(col("category") == "Memory Optimized")
  .filter(col("num_gpus") == 0)
  .filter(col("photon_driver_capable") == True)
  .filter(col("photon_worker_capable") == True)
  .filter(col("support_cluster_tags") == True)
  .filter(col("is_graviton") == False)
  .filter(col("is_hidden") == False)
  .filter(col("num_cores") >= 6)
  .orderBy(col("display_order"), col("memory_mb"), col("num_cores"))
  .select(col("node_type_id"))
  .limit(1)
  .collect()[0][0]
)
node_type_id

# COMMAND ----------

# DBTITLE 1,Dashboard Widget Configuration
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "synthea")
# dbutils.widgets.text("instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec")
dbutils.widgets.text("number_of_job_runs", "1", "Number of times to run the job")
dbutils.widgets.dropdown("create_landing_zone", "true", ["true", "false"], "Optional Create a landing zone")
dbutils.widgets.dropdown("inject_bad_data", "true", ["true", "false"], "Optional injection of bad data to select files")
dbutils.widgets.text(name = "min_records", defaultValue="1", label = "Minimum Generated Record Count")
dbutils.widgets.text(name = "max_records", defaultValue="1000", label = "Maximum Generated Record Count")
dbutils.widgets.dropdown(
  "serverless"
  ,"false"
  ,[ "false"] # must be false until after sreverless moves to 16 LTS release
  ,"Serverless Job Mode"
)
dbutils.widgets.dropdown("run_job", "true", ["true", "false"], "Optional Run the Job")

# COMMAND ----------

# DBTITLE 1,Retrieve Widget Inputs
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
# instance_pool_id = dbutils.widgets.get("instance_pool_id")
number_of_job_runs = int(dbutils.widgets.get("number_of_job_runs"))
create_landing_zone = dbutils.widgets.get("create_landing_zone").lower()
inject_bad_data = dbutils.widgets.get("inject_bad_data").lower()
min_records = int(dbutils.widgets.get("min_records"))
max_records = int(dbutils.widgets.get("max_records"))
serverless = dbutils.widgets.get("serverless").lower()
run_job = dbutils.widgets.get("run_job").lower()

# COMMAND ----------

# DBTITLE 1,File Writing Workflow Details
print(
f"""
Based on user input's the job will write files into this catalog.schema's Volume:
catalog_name = {catalog_name}
schema_name = {schema_name}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/

The Databricks workflow created by this notebook will create a "landing" zone: {create_landing_zone}
If create_landing_zone == True, the Databricks workflow created by this notebook will write files into the following schema's volume:
/Volumes/{catalog_name}/{schema_name}/landing/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook. 

Cluster Specification Details: 
node_type_id = {node_type_id}

Number of times the Databricks workflow will be executed to simulate variability in patient record creation: number_of_job_runs = {number_of_job_runs}
"""
)

# COMMAND ----------

# DBTITLE 1,Post Synthea Workflow to Databricks
post_job_result = dbutils.notebook.run(
  path = "workflows/synthea-on-dbx-create-workflow" 
  ,timeout_seconds = 150
  ,arguments = {
    "catalog_name": catalog_name
    ,"schema_name": schema_name
    ,"create_landing_zone": create_landing_zone
    # ,"instance_pool_id": instance_pool_id
    ,"node_type_id": node_type_id
    ,"inject_bad_data": inject_bad_data
    ,"min_records": min_records
    ,"max_records": max_records  
    ,"serverless": serverless
  }
)

# COMMAND ----------

# DBTITLE 1,Load JSON library
import json

# COMMAND ----------

# DBTITLE 1,Return Status and Job Id of Synthea Workflow Creation
if json.loads(post_job_result)["status"] == "OK":
  job_id = json.loads(post_job_result)["job"]["job_id"]
  print(f""" 
    Databricks Workflow Creation Successful, use job id: {job_id}.
  """)
else:
  raise Exception(f"""Databricks Workflow Creation Failed, please check the job run output for more information.""")

# COMMAND ----------

# DBTITLE 1,Import Random Number Generator
from random import randint

# COMMAND ----------

# DBTITLE 1,random wait time
wait_time = randint(60, 240)
wait_time

# COMMAND ----------

# DBTITLE 1,Import Sleep Method
from time import sleep

# COMMAND ----------

# DBTITLE 1,Job Scheduler
if run_job == "false":
  print("Skipping Job Run")
else:
  for i in range(0, number_of_job_runs):
    if i == 0:
      w.jobs.run_now_and_wait(
        job_id = job_id
        ,job_parameters = {
          "catalog_name": catalog_name
          ,"schema_name": schema_name
          ,"create_landing_zone": create_landing_zone
          ,"inject_bad_data": inject_bad_data
        } 
      )
    else:
      sleep(wait_time)
      w.jobs.run_now_and_wait(
        job_id = job_id
        ,job_parameters = {
          "catalog_name": catalog_name
          ,"schema_name": schema_name
          ,"create_landing_zone": create_landing_zone
          ,"inject_bad_data": inject_bad_data
        }
      )

# COMMAND ----------

# DBTITLE 1,List Job Runs
if run_job == "false":
  print("Job Run Skipped")
else:
  runs = w.jobs.list_runs(job_id=job_id)
  runs = [run.as_dict() for run in runs]

# COMMAND ----------

# DBTITLE 1,Import Pandas
import pandas as pd

# COMMAND ----------

# DBTITLE 1,List to Pandas to PySpark DataFrame Conversion
if run_job == "false":
  print("Job Run Skipped")
else:
  #create a pandas dataframe and then convert it to a pySpark dataframe
  runs_pandas = pd.DataFrame(runs)
  runs_pandas

# COMMAND ----------

# cleanup_result = dbutils.notebook.run(
#   path = "notebooks/02-clean-up"
#   ,timeout_seconds = 150
#   ,arguments = {
#     "catalog_name": catalog_name
#     ,"schema_name": schema_name
#   }
# )
# cleanup_result

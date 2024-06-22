# Databricks notebook source
# DBTITLE 1,install databricks sdk upgrade
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,get databricks-sdk version
# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

# DBTITLE 1,Dashboard Widget Configuration
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "synthea")
dbutils.widgets.text("instance_pool_id", "", "Optional Instance Pool ID for the Cluster Spec")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Node Type Id, Required if Instance Pool Id is not specified")
dbutils.widgets.text("number_of_job_runs", "1", "Number of times to run the job")
dbutils.widgets.dropdown("create_landing_zone", "false", ["true", "false"], "Optional Create a landing zone")

# COMMAND ----------

# DBTITLE 1,Retrieve Widget Inputs
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
instance_pool_id = dbutils.widgets.get("instance_pool_id")
node_type_id = dbutils.widgets.get("node_type_id")
number_of_job_runs = int(dbutils.widgets.get("number_of_job_runs"))
create_landing_zone = dbutils.widgets.get("create_landing_zone").lower()

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
instance_pool_id = {instance_pool_id}
node_type_id = {node_type_id}

Note that node_type_id will only be used if an instance_pool_id is not set.  Bricksters on e2-demo-field-eng may use instance_pool_id = 0727-104344-hauls13-pool-uftxk0r6.  

Number of times the Databricks workflow will be executed to simulate variability in patient record creation: number_of_job_runs = {number_of_job_runs}
"""
)

# COMMAND ----------

# DBTITLE 1,Databricks Workspace Client
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Post Synthea Workflow to Databricks
post_job_result = dbutils.notebook.run(
  path = "workflows/synthea-on-dbx-create-workflow"
  ,timeout_seconds = 150
  ,arguments = {
    "catalog_name": catalog_name
    ,"schema_name": schema_name
    ,"create_landing_zone": create_landing_zone
    ,"instance_pool_id": instance_pool_id
    ,"node_type_id": node_type_id
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
for i in range(0, number_of_job_runs):
  if i == 0:
    w.jobs.run_now_and_wait(
      job_id = job_id
      ,job_parameters = {
        "catalog_name": catalog_name
        ,"schema_name": schema_name
        ,"create_landing_zone": create_landing_zone
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
      } 
    )

# COMMAND ----------

# DBTITLE 1,List Job Runs
runs = w.jobs.list_runs(job_id=job_id)
runs = [run.as_dict() for run in runs]

# COMMAND ----------

# DBTITLE 1,Import Pandas
import pandas as pd

# COMMAND ----------

# DBTITLE 1,List to Pandas to PySpark DataFrame Conversion
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

# Databricks notebook source
# DBTITLE 1,Install Databricks SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Extract Databricks SDK version
# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

# DBTITLE 1,Input Catalog and Schema Name.
catalog_name = input("Catalog name (for example, main) for the job to write files into the schema's Volume: ")
schema_name = input("Schema name (for example, my_schema) that the Volume will be created in: ")

# COMMAND ----------

# DBTITLE 1,Catalog Schema Volume
print(
f"""
Based on user input's the job will write files into this catalog.schema's Volume:
catalog_name = {catalog_name}
schema_name = {schema_name}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook.
"""
)

# COMMAND ----------

# DBTITLE 1,Databricks SDK workspace initialization
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source

w = WorkspaceClient()

# COMMAND ----------

current_user = w.current_user.me()
latest_lts_version = w.clusters.select_spark_version(latest=True, long_term_support=True)

# COMMAND ----------



# COMMAND ----------

print(
f"""
Current user: {current_user.user_name}
Latest LTS version: {latest_lts_version}
"""
)

# COMMAND ----------


job_name            = input("Some short name for the job (for example, my-job): ")
description         = input("Some short description for the job (for example, My job): ")
existing_cluster_id = input("ID of the existing cluster in the workspace to run the job on (for example, 1234-567890-ab123cd4): ")
notebook_path       = input("Workspace path of the notebook to run (for example, /Users/someone@example.com/my-notebook): ")
task_key            = input("Some key to apply to the job's tasks (for example, my-key): ")

print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = job_name,
  tasks = [
    Task(
      description = description,
      existing_cluster_id = existing_cluster_id,
      notebook_task = NotebookTask(
        base_parameters = dict(""),
        notebook_path = notebook_path,
        source = Source("WORKSPACE")
      ),
      task_key = task_key
    )
  ]
)

print(f"View the job at {w.config.host}/#job/{j.job_id}\n")


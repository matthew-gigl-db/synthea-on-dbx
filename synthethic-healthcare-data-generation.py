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

# COMMAND ----------

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



# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

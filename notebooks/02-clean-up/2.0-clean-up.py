# Databricks notebook source
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get(name = "catalog_name")
schema_name = dbutils.widgets.get(name = "schema_name")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/"
print(f"""
  catalog_name = {catalog_name}
  schema_name = {schema_name}
  volume_path = {volume_path}
""")

# COMMAND ----------

import subprocess

# COMMAND ----------

help(subprocess.run)

# COMMAND ----------

cmd = f"rm -rf {volume_path}*"
result = subprocess.run(cmd, shell=True, check=True, capture_output=True)

# COMMAND ----------

result

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "volume_contents": dbutils.fs.ls(volume_path)
}))

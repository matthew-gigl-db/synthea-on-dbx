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

try:
    # Code that may raise an exception
    dbutils.fs.ls(f"{volume_path}synthea_config.txt")
    result = "True"  # Return 0 if it works
except:
    result = "False"  # Return 1 if an exception occurs

result  # Return the result

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'result', value = result)

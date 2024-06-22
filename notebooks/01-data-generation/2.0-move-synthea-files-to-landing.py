# Databricks notebook source
# MAGIC %md
# MAGIC ### Copy files from **_synthetic_files_raw_** to **_landing_**

# COMMAND ----------

# DBTITLE 1,set catalog, schema, and landing zone widgets
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

# DBTITLE 1,get widget values and set volume path
catalog_name = dbutils.widgets.get(name = "catalog_name")
schema_name = dbutils.widgets.get(name = "schema_name")
source_volume_path = f"/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/"
target_volume_path = f"/Volumes/{catalog_name}/{schema_name}/landing/"
print(f"""
  catalog_name = {catalog_name}
  schema_name = {schema_name}
  source_volume_path = {source_volume_path}
  target_volume_path = {target_volume_path}
""")

# COMMAND ----------

# DBTITLE 1,Creating a Volume Conditionally in SQL
# create landing zone volume if not exists
spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.landing')

# COMMAND ----------

# DBTITLE 1,Copy new files from synthetic_files_raw to landing zone
import os

# get directories and order by file name (timestamp) in ascending order (ensure correct processing order)
directories = spark.sql(f"LIST '{source_volume_path}/output/csv' ").orderBy("name")

# for each directory, get files and move them to landing
for directory in directories.collect():
  file_path = directory[0]
  directory = directory[1].split('/')[0]
  files = spark.sql(f"LIST '{file_path}' ")
  print(f"Copying files from directory: {directory} \n source:{file_path}  \n target:{target_volume_path}")

  # get files in given directory
  for file in files.collect():
    # create a folder for the csv based off of file name
    file_path = file[0]
    file_time = file_path.split('/')[-2]
    directory_name = file[1].split('.')[0]
    file_name = file_time + '_' + file[1].split('.')[0]
    
    # check if file exists and copy file
    dst = f"{target_volume_path}{directory_name}/{file_name}.csv"

    if os.path.exists(dst):
      print(f'File already exists, skipping file: {file_name}.csv')
    else:
      print(f'Copying file: {file_name}.csv to target: {target_volume_path}')
      dbutils.fs.cp(f"{file_path}", dst)
  print(f'Successfully copied files to target \n target: {target_volume_path}')

# COMMAND ----------

print(dst)

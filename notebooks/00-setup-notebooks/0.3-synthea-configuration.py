# Databricks notebook source
# MAGIC %md
# MAGIC # Synthea Configuration 
# MAGIC
# MAGIC The purpose of this notebook is to create a configuration file that will be used to set the parameters for each run of the Synthea Patient Generator.  
# MAGIC
# MAGIC Examples of some of the parameters that may be set: 
# MAGIC
# MAGIC * File formats including FHIR, C-CCA, simple text or CSV
# MAGIC * Append modes for CSV files
# MAGIC * Export folders
# MAGIC * Patient locations
# MAGIC * Population size
# MAGIC
# MAGIC For a full list of configuarion details please visit Synthethic Health's GitHub [page](https://github.com/synthetichealth/synthea).
# MAGIC
# MAGIC For this demo we're going to focus on setting consistent file type and file type settings for each run, and allow items like population size to be randomly set at run time.  This will make for a more realitic streaming scenario where the amount of patient records a health system may generate or recieve in any given time interval will vary.  

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Set Catalog, Schema, and Volume Paths 

# COMMAND ----------

# DBTITLE 1,set catalog and schema widgets
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

# DBTITLE 1,get widget values and set volume path
catalog_name = dbutils.widgets.get(name = "catalog_name")
schema_name = dbutils.widgets.get(name = "schema_name")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/"
print(f"""
  catalog_name = {catalog_name}
  schema_name = {schema_name}
  volume_path = {volume_path}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC *** 
# MAGIC ### Set Configuration Values 
# MAGIC
# MAGIC * **CCDA Export**: Generates CCDA formated files when set to *True*, defaulted to *False* in this notebook.  
# MAGIC * **FHIR Export**: Generates FHIR formated files when set to *True*, defaulted to *False* in this notehbook.  
# MAGIC * **CSV Export**: Generates CSV formated patient files when set to *True*. The rest of this demo will rely on the data model created from these CSV files, so this option defaults to *True* in this notebook.  
# MAGIC * **CSV Folder Per Run**: The default operation of Synthea is to overwrite any CSV files created.  Since we want to simulate patient records streaming into our system as multiple CSV files (one per each run) when can ensure we keep any existing files by setting this to *True*.  This will allow us to use the Databricks "cloudFiles" autoloader in our Delta Live Table (DLT) or Spark Structured Streaming pipelines later.  
# MAGIC * Set the **Base Directory** to dictate where the files should be saved, defaults to "./output/" where "." is the location of our JAR file used to execute Snythea.  

# COMMAND ----------

# DBTITLE 1,set synthea config widgets
dbutils.widgets.dropdown(name = "ccda", defaultValue = "false", choices = ("true", "false"), label="CCDA Export")
dbutils.widgets.dropdown(name = "fhir", defaultValue = "false", choices = ("true", "false"), label="FHIR Export")
dbutils.widgets.dropdown(name = "csv", defaultValue = "true", choices = ("true", "false"), label="CSV Export")
dbutils.widgets.dropdown(name = "csv_folder_per_run", defaultValue = "true", choices = ("true", "false"), label="CSV Folder Per Run")
dbutils.widgets.text(name = "destination", defaultValue="./output/", label = "Base Directory")

# COMMAND ----------

# MAGIC %md 
# MAGIC *** 
# MAGIC ### Write the Configuration File to Our Volume 

# COMMAND ----------

ccda = dbutils.widgets.get("ccda")
fhir = dbutils.widgets.get("fhir")
csv = dbutils.widgets.get("csv")
csv_folder_per_run = dbutils.widgets.get("csv_folder_per_run")
destination = dbutils.widgets.get("destination")
if fhir == "true":
  hospital_fhir = "true"
  practitioner_fhir = "true"
else:
  hospital_fhir = "false"
  practitioner_fhir = "false"

# COMMAND ----------

# DBTITLE 1,create config text
config_file_text = (
f"""# synthea streaming simulation configuration file
exporter.ccda.export = {ccda}
exporter.fhir.export = {fhir}
exporter.csv.export = {csv}
exporter.csv.folder_per_run = {csv_folder_per_run}
exporter.baseDirectory = {destination}
exporter.hospital.fhir.export = {hospital_fhir}
exporter.practitioner.fhir.export = {practitioner_fhir}
""")

print(config_file_text)

# COMMAND ----------

# DBTITLE 1,write config file
# Write config_file_text string variable to a file in our volume
filename = f"{volume_path}synthea_config.txt"

with open(filename, "w") as f:
    f.write(config_file_text)

f.close()

# COMMAND ----------

# DBTITLE 1,verify config file
print(dbutils.fs.head(f"{filename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ##### Optional: Test Config File 
# MAGIC
# MAGIC The below cell has been intentionally commented out.  

# COMMAND ----------

# DBTITLE 1,use config file in run
# command = (
# f"""cd {volume_path}
# java -jar synthea-with-dependencies.jar -c {filename} -p 5
# """)
# print(command)

# COMMAND ----------

# !{command}

# COMMAND ----------

# dbutils.fs.rm(f"{volume_path}output/", True)

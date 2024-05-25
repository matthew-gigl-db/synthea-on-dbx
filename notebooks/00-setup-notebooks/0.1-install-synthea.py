# Databricks notebook source
# MAGIC %md
# MAGIC # Install Synthea
# MAGIC
# MAGIC The purpose of this notebook is download and set up Synthethic Health's latest Synthea jar files from Github for use in creating our synthetic patient files that we will use as our data source.  
# MAGIC
# MAGIC For more infomration on the Synthea Patient Generator, please visit: [https://github.com/synthetichealth/synthea](https://github.com/synthetichealth/synthea)

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ## Enable Java JDK 17
# MAGIC
# MAGIC Synthea requires that we use either Java JDK 11 or 17 in order to execute commands with its latest release as a JAR file.  
# MAGIC
# MAGIC The default Java runtime on Databricks clusters is version 8, however starting with DBR 13, versions 11 and 17 are also installed.  The next LTS version of Java will be 17, therefore its recommended that we use Java 17 for applications like this whenever possible.   In order to use Java 17 on your cluster you'll need to set an environment variable in the cluster's **Advanced Options** section.
# MAGIC
# MAGIC  <img src="https://github.com/matthew-gigl-db/db-nosql/blob/main/images/Java17EnvVariable.png?raw=true"> 
# MAGIC
# MAGIC More information may be found here: [https://docs.databricks.com/en/dev-tools/sdk-java.html#create-a-cluster-that-uses-jdk-17](https://docs.databricks.com/en/dev-tools/sdk-java.html#create-a-cluster-that-uses-jdk-17)

# COMMAND ----------

# MAGIC %md
# MAGIC When done correctly, the following cell will output an open JDK runtime version of 17.X.X.  

# COMMAND ----------

# DBTITLE 1,check java version
# MAGIC %sh
# MAGIC java -version

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Set Catalog, Schema, and Volume Paths 

# COMMAND ----------

# DBTITLE 1,set db widgets
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

# DBTITLE 1,retrieve widget values
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
# MAGIC ### Retrieve the latest Synthea Release

# COMMAND ----------

# DBTITLE 1,import urlretrieve
from urllib.request import urlretrieve

# COMMAND ----------

# DBTITLE 1,download latest synthea release
urlretrieve(
  url = "https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar"
  ,filename = f"{volume_path}synthea-with-dependencies.jar"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Execute the JAR One Time to Initialize 

# COMMAND ----------

# DBTITLE 1,check installation
command = f"""
cd {volume_path}
java -jar synthea-with-dependencies.jar
"""

# COMMAND ----------

# DBTITLE 1,run command
# !{command}

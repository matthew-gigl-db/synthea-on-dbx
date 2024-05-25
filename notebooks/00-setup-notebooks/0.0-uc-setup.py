# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Set-Up 
# MAGIC
# MAGIC This notebook creates a catalog and schema that we'll use to create tables, plus a managed volume that will use to land the raw synthetic files that we'll generate from using Synthetic Health's Synthea package.  
# MAGIC
# MAGIC It's recommended to use a catalog name that you have read and write access to, or to create a new catalog (assuming that you have that permissoin in UC) to you'll be able to use to run the demo.  A brand new catalog is not required.  Please note that there is not a default setting for catalog, therefore you will need to supply a value in the Databricks widget.  You're welcome to create your own schema name in your catalog of choice.  The default schema name is "synthea" since we'll be generating synthetic files using the Synthea package.  You may need to change the schema name if more than one person in your organization is running this demo at the same time.  
# MAGIC
# MAGIC Since Unity Catalog uses a three level name space, the volume name "synthetic_files_raw" is techincally unique if either the catalog name or the schema name is unique.  For simplicity we'll be using a managed volume, though external volumes are also an option.  Our volume will have the path:  "/Volumes/<catalog_name>/<schema_name>/synthetic_files_raw/" 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create a catalog and a schema to house the synthethic patient data.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Databricks Widgets

# COMMAND ----------

# DBTITLE 1,widget assignment
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Create and Assign Catalog to Use

# COMMAND ----------

# DBTITLE 1,retrieve catalog name
catalog_name = dbutils.widgets.get(name = "catalog_name")
catalog_name

# COMMAND ----------

# DBTITLE 1,create catalog
# MAGIC %sql
# MAGIC create catalog if not exists ${catalog_name};

# COMMAND ----------

# DBTITLE 1,use and check catalog
# MAGIC %sql
# MAGIC use catalog ${catalog_name};
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and Assign Schema in the Catalog to Use 

# COMMAND ----------

# DBTITLE 1,retrieve schema name
schema_name = dbutils.widgets.get("schema_name")
schema_name

# COMMAND ----------

# DBTITLE 1,create schema in catalog
# MAGIC %sql
# MAGIC create schema if not exists ${schema_name};

# COMMAND ----------

# DBTITLE 1,use and check schema
# MAGIC %sql
# MAGIC use schema ${schema_name};
# MAGIC select current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the Volume 

# COMMAND ----------

# DBTITLE 1,create volume
# MAGIC %sql
# MAGIC create volume if not exists synthetic_files_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC Volumes may be accessed using standard shell commands, just like they were local drives to our cluster, despite really being on a distributed file system in a cloud data lake storage account.  This makes working with files on Databricks very easy.  The following cell shows the (typically empty) contents of the volume we just created.  

# COMMAND ----------

# DBTITLE 1,check volume
command = f"ls -R /Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/"
!{command}

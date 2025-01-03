-- Databricks notebook source
DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT "main";
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT "synthea";
DECLARE OR REPLACE VARIABLE drop_tables BOOLEAN DEFAULT true;

-- COMMAND ----------

SET VARIABLE catalog_name = :catalog_name; 
SET VARIABLE schema_name = :schema_name;
SET VARIABLE drop_tables = CASE WHEN :drop_tables = 'true' THEN true ELSE false END;  

-- COMMAND ----------

select catalog_name, schema_name, drop_tables;

-- COMMAND ----------

USE IDENTIFIER(catalog_name || "." || schema_name);

-- COMMAND ----------

SELECT current_catalog(), current_schema();

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE landing_volume_path STRING DEFAULT "/Volumes/" || catalog_name || "/" || schema_name || "/landing/";

SELECT landing_volume_path;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Allergies
-- MAGIC ***

-- COMMAND ----------

DROP TABLE IF EXISTS allergies_bronze; 

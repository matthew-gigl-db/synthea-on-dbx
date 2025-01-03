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
-- MAGIC ## CREATE STREAMING TABLES 
-- MAGIC *** 
-- MAGIC
-- MAGIC `{ CREATE OR REFRESH STREAMING TABLE | CREATE STREAMING TABLE [ IF NOT EXISTS ] }  
-- MAGIC   table_name  
-- MAGIC   [ table_specification ]  
-- MAGIC   [ table_clauses ]  
-- MAGIC   [ AS query ]`
-- MAGIC
-- MAGIC `table_specification
-- MAGIC   ( { column_identifier column_type [column_properties] } [, ...]
-- MAGIC     [ CONSTRAINT expectation_name EXPECT (expectation_expr)
-- MAGIC       [ ON VIOLATION { FAIL UPDATE | DROP ROW } ] ] [, ...]
-- MAGIC     [ , table_constraint ] [...] )`
-- MAGIC
-- MAGIC `column_properties
-- MAGIC   { NOT NULL |
-- MAGIC     COMMENT column_comment |
-- MAGIC     column_constraint |
-- MAGIC     MASK clause } [ ... ]`
-- MAGIC
-- MAGIC `table_clauses
-- MAGIC   { PARTITIONED BY (col [, ...]) |
-- MAGIC     COMMENT table_comment |
-- MAGIC     TBLPROPERTIES clause |
-- MAGIC     SCHEDULE [ REFRESH ] schedule_clause |
-- MAGIC     WITH { ROW FILTER clause } } [...]`
-- MAGIC
-- MAGIC `schedule_clause
-- MAGIC   { EVERY number { HOUR | HOURS | DAY | DAYS | WEEK | WEEKS } |
-- MAGIC   CRON cron_string [ AT TIME ZONE timezone_id ] }`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Allergies
-- MAGIC ***

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Bronze

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE table_name STRING;
DECLARE OR REPLACE VARIABLE bronze_table_name STRING; 

SET VARIABLE table_name = "allergies";
SET VARIABLE bronze_table_name = table_name || "_bronze";

SELECT table_name, bronze_table_name;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE drop_bronze_stmnt STRING; 
DECLARE OR REPLACE VARIABLE drop_table_stmnt STRING;

SET VARIABLE drop_bronze_stmnt = CASE 
  WHEN drop_tables = true THEN "DROP TABLE IF EXISTS " || bronze_table_name || ";"
  ELSE "SELECT 'Skipping drop bronze table statement.' AS message;" 
END;

SET VARIABLE drop_table_stmnt = CASE
  WHEN drop_tables = true THEN "DROP TABLE IF EXISTS " || table_name || ";"
  ELSE "SELECT 'Skipping drop table statement.' as message;"
END;

SELECT drop_bronze_stmnt, drop_table_stmnt;

-- COMMAND ----------

EXECUTE IMMEDIATE drop_bronze_stmnt;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE bronze_table_specification STRING;

SET VARIABLE bronze_table_specification = "
  (
    file_metadata STRUCT < file_path: STRING,
    file_name: STRING,
    file_size: BIGINT,
    file_block_start: BIGINT,
    file_block_length: BIGINT,
    file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.',
    ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.',
    value STRING COMMENT 'The raw CSV file contents.'
  )
"

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE bronze_table_clauses STRING; 

SET VARIABLE bronze_table_clauses = "
  COMMENT 'Raw snythethic patient data CSV files ingested from the landing volume for the " || table_name || " data set.'
  TBLPROPERTIES (
    'quality' = 'bronze'
  )
"

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE crst_bronze_stmnt STRING; 

SET VARIABLE crst_bronze_stmnt = "CREATE OR REFRESH STREAMING TABLE IDENTIFIER(bronze_table_name) 
" || bronze_table_specification || bronze_table_clauses || " 
AS SELECT
  _metadata as file_metadata
  ,* 
FROM STREAM read_files(
  '" || landing_volume_path || table_name || "/'
  ,format => 'csv'
  ,header => true
  ,schema => 'value STRING'
  ,delimiter => '~'
  ,multiLine => false
  ,encoding => 'UTF-8'
  ,ignoreLeadingWhiteSpace => true
  ,ignoreTrailingWhiteSpace => true
  ,mode => 'FAILFAST'
)";

SELECT crst_bronze_stmnt;

-- COMMAND ----------

EXECUTE IMMEDIATE crst_bronze_stmnt;

-- COMMAND ----------

SHOW CREATE TABLE IDENTIFIER(bronze_table_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Staging 

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE crtst_staging_stmnt STRING;

SET VARIABLE crtst_staging_stmnt = "
  CREATE OR REFRESH TEMPORARY STREAMING TABLE IDENTIFIER(" table_name ||"_staging) AS
  SELECT
    file_

"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Start	Date (YYYY-MM-DD)	true	The date the allergy was diagnosed.
-- MAGIC Stop	Date (YYYY-MM-DD)	false	The date the allergy ended, if applicable.
-- MAGIC 🗝️	Patient	UUID	true	Foreign key to the Patient.
-- MAGIC 🗝️	Encounter	UUID	true	Foreign key to the Encounter when the allergy was diagnosed.
-- MAGIC Code	String	true	Allergy code
-- MAGIC System	String	true	Terminology system of the Allergy code. RxNorm if this is a medication allergy, otherwise SNOMED-CT.
-- MAGIC Description	String	true	Description of the Allergy
-- MAGIC Type	String	false	Identify entry as an allergy or intolerance.
-- MAGIC Category	String	false	Identify the category as drug, medication, food, or environment.
-- MAGIC Reaction1	String	false	Optional SNOMED code of the patients reaction.
-- MAGIC Description1	String	false	Optional description of the Reaction1 SNOMED code.
-- MAGIC Severity1	String	false	Severity of the reaction: MILD, MODERATE, or SEVERE.
-- MAGIC Reaction2	String	false	Optional SNOMED code of the patients second reaction.
-- MAGIC Description2	String	false	Optional description of the Reaction2 SNOMED code.
-- MAGIC Severity2	String	false	Severity of the second reaction: MILD, MODERATE, or SEVERE.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE allergies_staging AS 
SELECT
  file_metadata.file_modification_time as sequence_key,
  parsed_value.start as start,
  parsed_value.stop as stop,
  parsed_value.patient_id as patient_id,
  parsed_value.encounter_id as encounter_id,
  parsed_value.code as code,
  parsed_value.system as system,
  parsed_value.description as description,
  parsed_value.type as type,
  parsed_value.category as category,
  parsed_value.reaction1 as reaction1,
  parsed_value.description1 as description1,
  parsed_value.severity1 as severity1,
  parsed_value.reaction2 as reaction2,
  parsed_value.description2 as description2,
  parsed_value.severity2 as severity2
FROM (
  SELECT
    file_metadata,
    ingest_time,
    from_csv(
      value,
      'start DATE, stop DATE, patient_id STRING, encounter_id STRING, code STRING, system STRING, description STRING, type STRING, category STRING, reaction1 STRING, description1 STRING, severity1 STRING, reaction2 STRING, description2 STRING, severity2 STRING',
      map(
        'sep', ',',
        'ignoreTrailingWhiteSpace', 'true',
        'ignoreLeadingWhiteSpace', 'true'
      )
    ) AS parsed_value
  FROM STREAM(allergies_bronze)
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE allergies
(
  start DATE NOT NULL COMMENT 'The date the allergy was diagnosed.'
  ,stop DATE COMMENT 'The date the allergy ended, if applicable.'
  ,patient_id STRING NOT NULL COMMENT 'Foreign key to the Patient table.'
  ,encounter_id STRING NOT NULL COMMENT 'Foreign key to the Encounter when the allergy was diagnosed.'
  ,code STRING NOT NULL COMMENT 'Allergy code'
  ,system STRING NOT NULL COMMENT 'Terminology system of the Allergy code. RxNorm if this is a medication allergy, otherwise SNOMED-CT.'
  ,description STRING NOT NULL COMMENT 'Description of the Allergy'
  ,type STRING COMMENT 'Identify entry as an allergy or intolerance'
  ,category STRING COMMENT 'Identify the category as drug, medication, food, or environment'
  ,reaction1 STRING COMMENT 'Optional SNOMED code of the patients reaction'
  ,description1 STRING COMMENT 'Optional description of the Reaction1 SNOMED code'
  ,severity1 STRING COMMENT 'Severity of the reaction: MILD, MODERATE, or SEVERE'
  ,reaction2 STRING COMMENT 'Optional SNOMED code of the patients second reaction'
  ,description2 STRING COMMENT 'Optional description of the Reaction2 SNOMED code'
  ,severity2 STRING COMMENT 'Severity of the second reaction: MILD, MODERATE, or SEVERE'
  ,CONSTRAINT fk_patient_id FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
  ,CONSTRAINT fk_encounter_id FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id)
)
COMMENT 'Allergies diagnosed for the patient as part of an encounter. Currently active allergies have a stop date of NULL. The composite key of this table is start, patient_id, encounter_id, code, system.'
TBLPROPERTIES (
  'quality' = 'silver'
);

APPLY CHANGES INTO allergies
FROM allergies_staging
KEYS (start, patient_id, encounter_id, code, system)
SEQUENCE BY sequence_key
COLUMNS * EXCEPT (sequence_key)
STORED AS SCD TYPE 2
TRACK HISTORY ON *;

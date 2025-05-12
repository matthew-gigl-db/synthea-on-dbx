-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT "mgiglia";
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT "synthea";
DECLARE OR REPLACE VARIABLE full_refresh BOOLEAN DEFAULT false;
DECLARE OR REPLACE VARIABLE table_name STRING DEFAULT "patients";

SET VARIABLE catalog_name = '${catalog_name}'; 
SET VARIABLE schema_name = '${schema_name}';
SET VARIABLE full_refresh = CASE WHEN '${full_refresh}' = 'true' THEN true ELSE false END;  
SET VARIABLE table_name = '${table_name}'; 

-- select catalog_name, schema_name, full_refresh, table_name;

USE IDENTIFIER(catalog_name || "." || schema_name);

-- SELECT current_catalog(), current_schema();

DECLARE OR REPLACE VARIABLE full_refresh_stmnt STRING; 

SET VARIABLE full_refresh_stmnt = CASE 
  WHEN full_refresh = true THEN "DROP TABLE IF EXISTS " || table_name || ";"
  ELSE "SELECT 'Performing Standard Refresh of " || table_name || ".' AS message;" 
END;

-- SELECT full_refresh_stmnt;

EXECUTE IMMEDIATE full_refresh_stmnt;

DECLARE OR REPLACE VARIABLE table_specification STRING;
DECLARE OR REPLACE VARIABLE table_clauses STRING;
DECLARE OR REPLACE VARIABLE table_select STRING;
DECLARE OR REPLACE VARIABLE table_keys STRING;
DECLARE OR REPLACE VARIABLE table_sequence_by STRING;
DECLARE OR REPLACE VARIABLE table_stored_as STRING;

SET VARIABLE table_specification = (SELECT ddl.specification from table_specifications where name = table_name);
SET VARIABLE table_clauses = (SELECT ddl.clauses from table_specifications where name = table_name);
SET VARIABLE table_select = (SELECT as_select from table_specifications where name = table_name);
SET VARIABLE table_keys = (SELECT pkeys from table_specifications where name = table_name);
SET VARIABLE table_sequence_by = (SELECT sequence_by from table_specifications where name = table_name);
SET VARIABLE table_stored_as = (SELECT stored_as from table_specifications where name = table_name);

DECLARE OR REPLACE VARIABLE crst_stmnt STRING; 

SET VARIABLE crst_stmnt = "CREATE OR REFRESH STREAMING TABLE " || table_name || " (\n" ||
table_specification || "\n)\n" ||
table_clauses || " 
AS SELECT " || table_select || ";\n
APPLY CHANGES INTO " || table_name || "
FROM " || table_name || "_bronze 
KEYS (" || table_keys || ")
SEQUENCE BY " || table_sequence_by || "
STORED AS " || table_stored_as || ";"
;

EXECUTE IMMEDIATE crst_stmnt;
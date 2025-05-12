-- this is a comment 
CREATE OR REFRESH STREAMING TABLE encounters_dlt (
 CONSTRAINT encounter_patient_id_null EXPECT (PATIENT IS NOT NULL)
)
COMMENT 'Patient encounters.' 
TBLPROPERTIES (
	'quality' = 'bronze'
	,'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
);

APPLY CHANGES INTO encounters_dlt
FROM STREAM(encounters_bronze)
KEYS (ID)
SEQUENCE BY file_metadata.file_modification_time
COLUMNS * EXCEPT (file_metadata)
STORED AS SCD TYPE 1;
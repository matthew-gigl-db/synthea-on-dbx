-- this is a comment 
CREATE OR REFRESH STREAMING TABLE encounters (
 CONSTRAINT encounter_patient_id_null EXPECT (PATIENT IS NOT NULL)
)
COMMENT 'Patient encounters.' 
TBLPROPERTIES (
	'quality' = 'bronze'
	,'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
);

APPLY CHANGES INTO encounters
FROM STREAM(encounters_stage)
KEYS (encounter_id)
SEQUENCE BY file_metadata.file_modification_time
COLUMNS * EXCEPT (file_metadata, ingest_time)
STORED AS SCD TYPE 1;
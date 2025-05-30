-- Create or Refresh Target Streaming Table 
CREATE OR REFRESH STREAMING TABLE encounters (
	CONSTRAINT encounter_patient_id_null EXPECT (patient_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Patient encounters.' 
TBLPROPERTIES (
	'quality' = 'silver'
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

CREATE OR REFRESH STREAMING TABLE encounters_quarantine (
	CONSTRAINT encounter_patient_id_not_null EXPECT (patient_id IS NULL) ON VIOLATION DROP ROW
)
COMMENT 'Patient encounters quarantine table where records should be reviewed.' 
TBLPROPERTIES (
	'quality' = 'bronze'
	,'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
)
AS SELECT * FROM STREAM (encounters_stage);


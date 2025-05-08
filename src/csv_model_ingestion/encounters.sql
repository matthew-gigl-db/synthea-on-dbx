CREATE OR REFRESH STREAMING TABLE encounters_dlt (
 encounter_id STRING,
 encounter_start_date TIMESTAMP,
 encounter_end_date TIMESTAMP,
 patient_id STRING,
 organization_id STRING,
 provider_id STRING,
 payer_id STRING,
 encounter_class STRING,
 code BIGINT,
 description STRING,
 base_encounter_cost DOUBLE,
 total_claim_cost DOUBLE,
 payer_coverage DOUBLE,
 reason_code BIGINT,
 reason_description STRING,
 _rescued_data STRING,
 CONSTRAINT encounter_patient_id_null EXPECT (patient_id IS NOT NULL)
)
COMMENT 'Patient encounters.' 
TBLPROPERTIES (
	'quality' = 'bronze'
	,'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
);

APPLY CHANGES INTO encounters_dlt
FROM encounters_bronze 
KEYS (ID)
SEQUENCE BY file_metadata.file_modification_time
COLUMNS * EXCEPT (file_metadata)
STORED AS SCD TYPE 1;
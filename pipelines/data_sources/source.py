# The 'data_sources' folder contains definitions for all data sources
# used by the pipeline.

# Keeping 'data_sources' separate provides a clear overview of the data used
# and allows for easy swapping of sources during development.

from utilities import bronze.Bronze as Bronze

resource_types = ['patients', 'encounters', 'claims_transactions', 'conditions', 'medications']

for resource_type in resource_types:
    Bronze_pipeline = Bronze(
        spark = spark
        ,catalog = spark.conf.get("catalog_use")
        ,schema = spark.conf.get("schema_use")
        ,volume = spark.conf.get("volume_use")
        ,volume_sub_path = spark.conf.get("volume_sub_path_use")
        ,resource_type = resource_type)
    
    Bronze_pipeline.stream_ingest()
# The 'data_sources' folder contains definitions for all data sources
# used by the pipeline.

# Keeping 'data_sources' separate provides a clear overview of the data used
# and allows for easy swapping of sources during development.

from utilities.bronze import Bronze

resource_types = spark.conf.get("resource_types").split(',')
resource_types = [resource_type.strip() for resource_type in resource_types]

for resource_type in resource_types:
    BronzePipeline = Bronze(
        spark = spark
        ,catalog = spark.conf.get("catalog_use")
        ,schema = spark.conf.get("schema_use")
        ,volume = spark.conf.get("volume_use")
        ,volume_sub_path = spark.conf.get("volume_sub_path_use")
        ,resource_type = resource_type)
    
    BronzePipeline.stream_ingest()
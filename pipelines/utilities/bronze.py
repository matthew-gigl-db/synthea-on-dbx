import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class Bronze:
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, volume_sub_path: str, resource_type: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_sub_path = volume_sub_path
        self.resource_type = resource_type
    """
    The Bronze class represents a data structure for managing metadata related to a specific data resource.
    
    Attributes:
        spark (SparkSession): The SparkSession object used for interacting with the Spark runtime.
        catalog (str): The catalog name where the data is stored.
        schema (str): The schema name within the catalog.
        volume_sub_path (str): The sub-path within the volume where the data is located.
        resource_type (str): The type of the data resource.

    Methods:
        __repr__(): Returns a string representation of the Bronze object.
        stream_ingest(): Defines a Delta Live Table for streaming ingestion of CSV files.
        to_dict(): Converts the Bronze object attributes to a dictionary.
        from_dict(cls, data): Creates a Bronze object from a dictionary.
    """

    def __repr__(self):
        return f"Bronze(catalog='{self.catalog}', schema='{self.schema}', volume='{self.volume}',volume_sub_path='{self.volume_sub_path}', resource_type='{self.resource_type}')"
      
    def stream_ingest(self):
      schema_definition = f"""
        file_metadata STRUCT < file_path: STRING, 
        file_name: STRING,
        file_size: BIGINT,
        file_block_start: BIGINT,
        file_block_length: BIGINT,
        file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.',ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.',
        value STRING COMMENT 'The raw CSV file contents.'
      """

      if self.volume_sub_path == None:
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.resource_type}"
      else:
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.volume_sub_path}/{self.resource_type}"

      @dlt.table(
        name=f"{self.catalog}.{self.schema}.{self.resource_type}_bronze",
        comment=f"Streaming bronze ingestion of {self.resource_type} CSV files.",
        # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
        table_properties={
          'quality' : 'bronze'
          ,'delta.enableChangeDataFeed' : 'true'
          ,'delta.enableDeletionVectors' : 'true'
          ,'delta.enableRowTracking' : 'true'
        },
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        cluster_by = ["file_metadata.file_path"],
        schema=schema_definition,
        # row_filter = "row-filter-clause",
        temporary=False
      )
      # @dlt.expect(...)
      def stream_ingest_function():
          return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("clusterByAuto", "true")
            .load(volume_path)
            .withColumn("file_metadata", col("_metadata"))
          )

    def to_dict(self):
        return {"spark": self.spark, "catalog": self.catalog, "schema": self.schema, "volume_sub_path": self.volume_sub_path, "resource_type": self.resource_type}

    @classmethod
    def from_dict(cls, data):
        return cls(data['spark'], data['catalog'], data['schema'], data['volume_sub_path'], data['resource_type'])









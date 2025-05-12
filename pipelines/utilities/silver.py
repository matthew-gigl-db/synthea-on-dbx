import dlt
from spark.sql import Sparksession
from spark.sql.functions import col

class Silver:
  def __init__(self, spark: SparkSession, table_definition: str):
    self.spark = spark
    ,self.bronze_table = bronze_table
    ,self.silver_table = silver_table
    """
    The Silver class is responsible for transforming data from the bronze table to the silver table.
    
    Attributes:
        spark (SparkSession): The SparkSession object used for interacting with the Spark runtime.
        bronze_table (str): The name of the bronze table containing raw data.
        silver_table (str): The name of the silver table where transformed data will be stored.
    """

    def __repr__(self):
        return f"Silver(bronze_table='{self.bronze_table}', silver_table='{self.silver_table}')"
      
    def transform_and_stage(self):
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
        # cluster_by = ["colname_1", "colname_2"],
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
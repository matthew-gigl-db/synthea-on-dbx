import dlt
from pyspark.sql import Sparksession
from pyspark.sql.functions import col

class Silver:
  def __init__(self, spark: SparkSession, table_definition: dict):
    self.spark = spark,
    self.table_definition = table_definition
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
      source = f"{self.table_definition['name']}_bronze"
      name = f"{self.table_definition['name']}_stage"
      comment = f"Staged {self.table_definition['name']} data."
      table_properties = self.table_definition['clauses']['table_properties']
      schema = """file_metadata STRUCT < file_path: STRING, 
        file_name: STRING,
        file_size: BIGINT,
        file_block_start: BIGINT,
        file_block_length: BIGINT,
        file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.'
        ,ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.',
        """ + self.table_definition['ddl']['schema']

      @dlt.table(
        name=name
        ,comment=comment
        # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
        ,table_properties=table_properties
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        # cluster_by = ["colname_1", "colname_2"],
        ,schema=schema
        # row_filter = "row-filter-clause",
        ,temporary=True
      )
      # @dlt.expect(...)
      def transform_and_function():
          return (self.spark.readStream
            .table(source)
            .option("clusterByAuto", "true")
            .withColumn("data", from_csv(col("value"), schema))
            .select(file_metadata, ingest_time, "data.*")
          )

    def to_dict(self):
        return {"spark": self.spark, "table_definition": self.table_definition}

    @classmethod
    def from_dict(cls, data):
        return cls(data['spark'], data['table_definition'])
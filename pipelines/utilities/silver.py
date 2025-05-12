import dlt
from spark.sql import Sparksession
from spark.sql.functions import col

class Silver:
  def __init__(self, spark: SparkSession, bronze_table: str, silver_table: str):
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
  
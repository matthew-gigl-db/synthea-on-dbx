# Databricks notebook source
dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get(name = "catalog_name")
schema_name = dbutils.widgets.get(name = "schema_name")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/output/csv/"
print(f"""
  catalog_name = {catalog_name}
  schema_name = {schema_name}
  volume_path = {volume_path}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define functions for creating bad data
# MAGIC - null values
# MAGIC - negative values

# COMMAND ----------

# DBTITLE 1,Define data quality functions
import csv
import numpy as np
import random
import string
import os

def introduce_nulls(row, columns, null_fraction=0.05):
  """
  Introduce nulls into a row and column with a given null fraction.

  Parameters:
  row (dict): A dictionary representing a row of data, with column names as keys.
  columns (list): A list of column names where null values should be introduced.
  null_fraction (float): The fraction of values to be set as null in the specified columns. Should be between 0 and 1.

  Returns: 
  dict: The modified row with null values introduced in the specified columns.
  """
  for i in columns:
        if np.random.rand() < null_fraction:
            row[i] = ''
  return row

def introduce_negative_values(row, columns, null_fraction=0.05):
  """
  Introduce nulls into a row and column with a given null fraction.

  Parameters:
  row (dict): A dictionary representing a row of data, with column names as keys.
  columns (list): A list of column names where null values should be introduced.
  null_fraction (float): The fraction of values to be set as null in the specified columns. Should be between 0 and 1.

  Returns: 
  dict: The modified row with null values introduced in the specified columns.
  """
  for i in columns:
        if np.random.rand() < null_fraction:
            row[i] = row[i] * -1
  return row
  
# function to read specified file and write it with bad data
def read_file_write_bad_data(file_path, null_columns, neg_columns, null_fraction=.03):
  # Read the original file and write the modified content back to the same file
  with open(file_path, 'r') as infile:
      reader = csv.DictReader(infile)
      fieldnames = reader.fieldnames  # Get the header from the first row
      rows = [row for row in reader]

  # Introduce bad data into each row
  modified_rows = []
  for row in rows:
      row = introduce_nulls(row, columns=null_columns, null_fraction=null_fraction)
      row = introduce_negative_values(row, columns=neg_columns, null_fraction=null_fraction)
      modified_rows.append(row)

  # Write the header and modified rows back to the same file
  with open(file_path, 'w', newline='') as outfile:
      writer = csv.DictWriter(outfile, fieldnames=fieldnames)
      writer.writeheader()  # Write the header
      writer.writerows(modified_rows)  # Write the modified data rows

# define success file
def create_success_file(directory_path, file_name="SUCCESS.txt"):
  
  # Ensure the folder exists
  directory_path = directory_path + '/data_quality_output'
  if not os.path.exists(directory_path):
      os.makedirs(directory_path)

  # Create the success file if it doesn't already exist
  file_path = os.path.join(directory_path, file_name)
  if not os.path.exists(file_path):
    file_text = 'SUCCESS: Successfully added data quality issues to files'
    with open(file_path, 'w') as file:
        file.write(file_text)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply data quality functions to csv files in synthea_files_raw

# COMMAND ----------

# get directories and order by file name (timestamp) in ascending order (ensure correct processing order)
directories = spark.sql(f"LIST '{volume_path}' ").orderBy("name")

# for each directory, get files and move them to landing
for directory in directories.collect():
  directory_path = directory[0]
  directory = directory[1].split('/')[0]
  files = spark.sql(f"LIST '{directory_path}' ")
  success_file_exists = os.path.exists(directory_path+'/data_quality_output/SUCCESS.txt') 
  
  
  # check if success file exists, if it does then skip the directory
  if success_file_exists:
    print(f"Null and/or negative values have already been added to files in the directory:\n directory: {directory}. Skipping this directory...")
  else:
    print(f"Updating files in directory: {directory}")
    # get files in given directory
    for file in files.collect():
      # process data for every file
      file_path = file[0]
      file_name = file[1]

      ##############
      # encounters #
      ##############
      if file_name == 'encounters.csv':
        print(f'Applying null and/or negative values to {file_name} file')
        null_columns = ['PATIENT']
        neg_columns = ['PAYER_COVERAGE']
        # read csv file and write bad data
        read_file_write_bad_data(file_path,\
          null_columns=null_columns,\
          neg_columns=neg_columns,\
          null_fraction=random.uniform(0.01, 0.05)\
            )
        print(f'Finished applying null and/or negative values to {file_name} file to {null_fraction * 100}% of values')

      ##########
      # claims #
      ##########
      if file_name == 'claims.csv':
        print(f'Applying null and/or negative values to {file_name} file')
        null_columns = ['Id', 'PATIENTID', 'PROVIDERID']
        neg_columns = []
        null_fraction =.03\
        # read csv file and write bad data
        read_file_write_bad_data(file_path,\
          null_columns=null_columns,\
          neg_columns=neg_columns,\
          null_fraction=random.uniform(0.01, 0.05)\
            )
        print(f'Finished applying null and/or negative values to {file_name} file to {null_fraction * 100}% of values')

      #######################
      # claims_transactions #
      #######################
      if file_name == 'claims_transactions.csv':
        print(f'Adding null and/or negative values to {file_name} file')
        null_columns = []
        neg_columns = ['PAYMENTS']
        # read csv file and write bad data
        read_file_write_bad_data(file_path,\
          null_columns=null_columns,\
          neg_columns=neg_columns,\
          null_fraction=random.uniform(0.01, 0.05)\
            )
        print(f'Finished applying null and/or negative values to {file_name} file to {null_fraction * 100}% of values')      

      ##############
      # conditions #
      ##############
      if file_name == 'conditions.csv':
        print(f'Applying null and/or negative values to {file_name} file')
        null_columns = ['PATIENT', 'ENCOUNTER']
        neg_columns = []
        # read csv file and write bad data
        read_file_write_bad_data(file_path,\
          null_columns=null_columns,\
          neg_columns=neg_columns,\
          null_fraction=random.uniform(0.01, 0.05)\
            )
        print(f'Finished applying null and/or negative values to {file_name} file to {null_fraction * 100}% of values')  

      ###############
      # medications #
      ###############
      if file_name == 'medications.csv':
        print(f'Applying null and/or negative values to {file_name} file')
        null_columns = []
        neg_columns = ['TOTALCOST']
        # read csv file and write bad data
        read_file_write_bad_data(file_path,\
          null_columns=null_columns,\
          neg_columns=neg_columns,\
          null_fraction=random.uniform(0.01, 0.05)\
            )
        print(f'Finished applying null and/or negative values to {file_name} file to {null_fraction * 100}% of values')

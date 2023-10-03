# Databricks notebook source
# DBTITLE 1,List of defined functions
# MAGIC %md
# MAGIC General utility functions (Use \_\_doc\_\_ property of any function to get usage details):
# MAGIC - zero_pad_front
# MAGIC - clean_string
# MAGIC - get_valid_date_bounds
# MAGIC - get_random_file_name
# MAGIC - get_permanent_tables_list
# MAGIC - get_field_search_query_string
# MAGIC - write_data_frame_to_sandbox
# MAGIC - insert_data_frame_into_table
# MAGIC - write_data_frame_to_sandbox_delta_table
# MAGIC - insert_data_frame_into_delta_table
# MAGIC - delete_table_from_sandbox

# COMMAND ----------

import re, json
import requests
import numpy as np
import pandas as pd
import datetime, dateutil
import random, math, statistics
import matplotlib.pyplot as plt

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Zero-pad the front of an arbitrary string/integer
def zero_pad_front(id_, total_length=3):
  """Zero-pad the front of an arbitrary string/int input to a specified length

  Parameters:
  id_ (str or int): An arbitrary input string or integer
  total_length (int): Truncate (from the front) the result to this length

  Returns:
  str: Zero-padded string or integer, truncated to total_length

  """
  string_id = str(id_)
  return (total_length*'0' + string_id)[-total_length:]

# Define UDF
zero_pad_front_udf = F.udf(zero_pad_front, StringType())

# Register UDF so it can be used in SQL statements
spark.udf.register("zero_pad_front", zero_pad_front)

# COMMAND ----------

# DBTITLE 1,Clean (preprocess) string for text analysis
# Define UDF to clean lab result string for classification
def clean_string(input_string):
  # Return empty string if input_string is None
  if input_string is None:
    return ''
  
  # Characters to remove
  remove_char = ['.', '*', ':', '-', '(', ')', '/', ',', '+', '<', '>', "'"]
  
  # Remove non-alphanumeric characters
  cleaned_string = input_string
  for char_ in remove_char:
    cleaned_string = cleaned_string.replace(char_, '')
  
  # Lowercase, remove leading and trailing whitespace, and replace duplicate spaces within the string
  cleaned_string = re.sub(' +', ' ', cleaned_string.strip().lower())
  
  return cleaned_string
clean_string_udf = F.udf(clean_string, StringType())

# COMMAND ----------

# DBTITLE 1,Validate date bounds
# Validate or generate valid date bounds (as strings)
def get_valid_date_bounds(bounds):
  # Check if bounds is one of the required types and of proper length
  valid_types = [list, tuple]
  is_valid_type = any([isinstance(bounds, t) for t in valid_types])
  is_valid_length = False if not(is_valid_type) else len(bounds) > 1
  
  # Return None if bounds are invalid
  if (not(is_valid_type) or not(is_valid_length)):
    return None
  
  # Utility function to check if date is correct format
  def is_valid_date(d):
    try:
      datetime.datetime.strptime(str(d), '%Y-%m-%d')
    except (ValueError):
      return False
    return True
  
  # First two items must be valid dates, otherwise return None
  valid_dates = [is_valid_date(d) for d in bounds][:2]
  return bounds[:2] if all(valid_dates) else None

# COMMAND ----------

# DBTITLE 1,Get a random file name (for caching)
def get_random_file_name():
  """Generate a random file name based on current time stamp and a random number
  
  Parameters:
  None
  
  Returns:
  str: random file name
  
  """
  # Generate string based on current timestamp
  current_timestamp = list(datetime.datetime.now().timetuple())[:6]
  current_timestamp = [
    str(t) if i == 0 else zero_pad_front(t, 2) for i, t in enumerate(current_timestamp)]
  current_timestamp = ''.join(current_timestamp)
  
  # Get a random number to use in filename
  random_number = str(random.random()).split('.')[-1]
  
  return '_'.join([current_timestamp, random_number])

# COMMAND ----------

# DBTITLE 1,Get list of permanent tables in the specified sandbox
def get_permanent_tables_list(sandbox_db='rdp_phi_sandbox'):
  """Get list of permanent tables in specified database
  
  Parameters:
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  
  Returns:
  list: List of permanent tables in specified sandbox db
  
  """
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  permanent_tables_df = spark.sql(
    "SHOW TABLES IN {}".format(sandbox_db)).where(F.col('isTemporary') == False)
  permanent_tables_list = [row.tableName for row in permanent_tables_df.collect()]
  
  return permanent_tables_list

# COMMAND ----------

# DBTITLE 1,Get field search query string
# 'field' is the table field to search, 'search_list' is a list of strings to search for
# and 'leading', if True, indicates that matches must start with the search string
def get_field_search_query_string(field, search_list, leading=False):
  if (leading):
    query_string_list = ["({0} LIKE '{1}%')".format(field, s) for s in search_list]
    return ' OR '.join(query_string_list)
  else:
    query_string_list = ["({0} LIKE '%{1}%')".format(field, s) for s in search_list]
    return ' OR '.join(query_string_list)

# COMMAND ----------

# DBTITLE 1,Write dataframe to sandbox table
def write_data_frame_to_sandbox(df, table_name, sandbox_db='rdp_phi_sandbox', replace=False):
  """Write the provided dataframe to a sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be written to the sandbox
  table_name (str): Name of table to write
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  replace (bool): Whether to replace (True) the table if it exists
  
  Returns:
  (bool): True/False value indicating whether the table was successfully written
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    if(replace):
      # Drop existing table
      print("Deleting existing table '{}'...".format(full_table_name))
      spark.sql("""drop table {}""".format(full_table_name))
    else:
      # If table exists and replace is False, issue a warning and return False (do not overwrite)
      print("Warning: Table '{}' already exists!".format(full_table_name))
      print("If you are sure you want to replace, rerun with 'replace' set to True.")
      return False
  
  # Make sure there is no folder remaining from past save attempts
  db_path = sandbox_db.replace('_', '-')
  #########################################################################################
  # Arrival at this point means the table does not exist. Write the new table.
  broadcast_timeout = 1200
  print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
  spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
  print("Writing table '{0}'...".format(full_table_name))

  df.write.mode("overwrite").saveAsTable(full_table_name)
  
  # Run commands to optimize new table
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True

# COMMAND ----------

# DBTITLE 1,Insert records into sandbox table
def insert_data_frame_into_table(df, table_name, sandbox_db='rdp_phi_sandbox'):
  """Insert/merge records in provided dataframe into an existing sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be merged into sandbox table
  table_name (str): Name of table into which to merge records
  sandbox_db (str): Name of sandbox database (generally, this should be rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully merged
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Merge records into table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    broadcast_timeout = 1200
    print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
    spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
    print("Inserting records into table '{}'...".format(full_table_name))
    df.write.insertInto(".".join([sandbox_db, table_name]), overwrite=False)
  else:
    print("Specified table '{}' does not exist. Cannot insert records.".format(full_table_name))
    return False
    
  # Run commands to optimize new table
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True

# COMMAND ----------

# DBTITLE 1,Write dataframe to sandbox delta table
def write_data_frame_to_sandbox_delta_table(
  df, table_name, sandbox_db='rdp_phi_sandbox', replace=False):
  """Write the provided dataframe to a sandbox delta table
  
  Parameters:
  df (PySpark df): A dataframe to be written to the sandbox
  table_name (str): Name of table to write
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  replace (bool): Whether to replace (True) the table if it exists
  
  Returns:
  (bool): True/False value indicating whether the table was successfully written
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    if(replace):
      # Drop existing table
      print("Deleting existing table '{}'...".format(full_table_name))
      spark.sql("""drop table {}""".format(full_table_name))
    else:
      # If table exists and replace is False, issue a warning and return False (do not overwrite)
      print("Warning: Table '{}' already exists!".format(full_table_name))
      print("If you are sure you want to replace, rerun with 'replace' set to True.")
      return False
    
  # Arrival at this point means the table does not exist. Write the new table.
  broadcast_timeout = 1200
  print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
  print("Writing table '{0}'...".format(full_table_name))
  spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
  
  # Write the delta table
  df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
  
  # Run commands to optimize new table
  print("Optimizing table...")
  spark.sql("OPTIMIZE {}".format(full_table_name))
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True

# COMMAND ----------

# DBTITLE 1,Insert records into sandbox delta table
def insert_data_frame_into_delta_table(df, table_name, sandbox_db='rdp_phi_sandbox'):
  """Insert/merge records in provided dataframe into an existing sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be merged into sandbox table
  table_name (str): Name of table into which to merge records
  sandbox_db (str): Name of sandbox database (generally, this should be rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully merged
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Get full file path to data files
  full_table_path = \
    "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/{0}.db/{1}".format(
    sandbox_db, table_name)
  
  # Merge records into table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    broadcast_timeout = 1200
    print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
    spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
    print("Inserting records into table '{}'...".format(full_table_name))
    df.write.format("delta").mode("append").save(full_table_path)
  else:
    print("Specified table '{}' does not exist. Cannot insert records.".format(full_table_name))
    return False
    
  # Run commands to optimize new table
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True

# COMMAND ----------

# DBTITLE 1,Delete table from sandbox
def delete_table_from_sandbox(table_name, sandbox_db='rdp_phi_sandbox'):
  """Delete the specified table from the sandbox
  
  Parameters:
  table_name (str): Name of table to delete
  sandbox_db (str): Name of sandbox database containing the table (generally, this should be
                    rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully deleted
  
  """
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    # Drop existing table
    print("Deleting specified table '{}'...".format(full_table_name))
    spark.sql("""drop table {}""".format(full_table_name))
  else:
    # If table does not exist, print a notification
    print("NOTE: Table '{}' does not exist. Cannot delete.".format(full_table_name))
    return False
  
  # Make sure there is no folder remaining after delete
  db_path = sandbox_db.replace('_', '-')
  dummy = dbutils.fs.rm("abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/{1}.db/{2}".format(sandbox_db, table_name), True)
  
  return True
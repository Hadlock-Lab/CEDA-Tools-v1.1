# Databricks notebook source
# DBTITLE 1,Load CEDA ETL Tools
# MAGIC %run
# MAGIC "./load_ceda_etl_tools"

# COMMAND ----------

# DBTITLE 1,Specify the version number, whether to use today's date or manual input date
## Read the current date as a version string
from pyspark.sql.functions import current_date
version_number = str(spark.range(1).withColumn("date",current_date()).select("date").collect()[0][0]).replace('-', '_')
print("The version number added will be {}".format(version_number))

## Or manually load a version number
## Notice then remember to comment out this following part if you don't want the version number to be overwritten
version_number = "2023_05_31"
print("The manual selected version number added will be {}".format(version_number))

# COMMAND ----------

# DBTITLE 1,Set date bounds for ETL
# Set date bounds for ETL
date_bounds = ['2008-01-01', '2023-05-31']

# COMMAND ----------

# DBTITLE 1,ETL CEDA Encounter Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_encounters_{}'.format(version_number)

# Get encounter records for specified date bounds
encounter_df = get_encounter_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(encounter_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(encounter_df, output_table_name, replace=False)

# COMMAND ----------

# DBTITLE 1,ETL CEDA Problem List Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_problem_list_{}'.format(version_number)

# Get problem list records for specified date bounds
problem_list_df = get_problem_list_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(problem_list_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(problem_list_df, output_table_name, replace=False)

# COMMAND ----------

# DBTITLE 1,ETL CEDA Procedure Order Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_procedure_orders_{}'.format(version_number)

# Get procedure order records for specified date bounds
procedure_order_df = get_procedure_order_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(procedure_order_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(procedure_order_df, output_table_name, replace=False)

# COMMAND ----------

# DBTITLE 1,ETL CEDA Medication Order Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_medication_orders_{}'.format(version_number)

# Get medication order records for specified date bounds
medication_order_df = get_medication_order_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(medication_order_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(medication_order_df, output_table_name, replace=False)

# COMMAND ----------

# DBTITLE 1,ETL CEDA Flowsheets Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_flowsheets_{}'.format(version_number)

# Get flowsheet records for specified date bounds
flowsheets_df = get_flowsheet_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(flowsheets_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(flowsheets_df, output_table_name, replace=False)

# COMMAND ----------

# DBTITLE 1,ETL CEDA Summary Block Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_summary_block_{}'.format(version_number)

# Get summary block records for specified date bounds
summary_block_df = get_summary_block_table(table_prefix=None, date_bounds=date_bounds)

# Write results to sandbox
if (output_table_name in get_permanent_tables_list()):
  insert_data_frame_into_delta_table(summary_block_df, output_table_name)
else:
  write_data_frame_to_sandbox_delta_table(summary_block_df, output_table_name, replace=False)
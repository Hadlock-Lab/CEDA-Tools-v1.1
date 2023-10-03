# Databricks notebook source
# DBTITLE 1,Load CEDA ETL Tools
# MAGIC %run
# MAGIC "./load_ceda_etl_tools"

# COMMAND ----------

# DBTITLE 1,Specify the latest omop concept tables (Notice: need to manually check the version of those tables, since it is only manually update every 3-6 months)
## Previous omop_versions are:
# omop_ver = "2022_02_11", "2022_06_27"

## latest omop_version is:
omop_ver = "2022_11_07"

# COMMAND ----------

# DBTITLE 1,Specify whether to replace all mapping tables, Specify the version number, whether to use today's date or manual input date
## Set True if you have already updated both the OMOP and core tables needed
## default option is False
replace_all_mapping_tables = True

## Read the current date as a version string
from pyspark.sql.functions import current_date
version_number = str(spark.range(1).withColumn("date",current_date()).select("date").collect()[0][0]).replace('-', '_')
print("The version number added will be {}".format(version_number))

## Or manually load a version number
## Notice then remember to comment out this following part if you don't want the version number to be overwritten
version_number = "2023_05_31"
print("The manual selected version number added will be {}".format(version_number))

# COMMAND ----------

# DBTITLE 1,ETL Diagnosis ID / SNOMED Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_diagnosis_id_snomed_map_{}'.format(version_number)

# Get Diagnosis ID / SNOMED map
diagnosis_id_snomed_map_df = get_diagnosis_id_snomed_mapping(omop_table='omop_concept_{}'.format(omop_ver))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(diagnosis_id_snomed_map_df, output_table_name, replace=replace_all_mapping_tables)

# COMMAND ----------

# DBTITLE 1,ETL SNOMED / Diagnosis ID Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_snomed_diagnosis_id_map_{}'.format(version_number)

# Get SNOMED / Diagnosis ID map
snomed_diagnosis_id_map_df = get_snomed_diagnosis_id_mapping(
  diagnosis_id_snomed_map='hadlock_diagnosis_id_snomed_map_{}'.format(version_number),
  enc_diag_table='hadlock_encounters_{}'.format(version_number))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(snomed_diagnosis_id_map_df, output_table_name, replace=replace_all_mapping_tables)

# COMMAND ----------

# DBTITLE 1,ETL Medication ID / RxNorm Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_medication_id_rxnorm_map_{}'.format(version_number)

# Get Medication ID / RxNorm map
medication_id_rxnorm_map_df = get_medication_id_rxnorm_mapping(omop_table='omop_concept_{}'.format(omop_ver))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(medication_id_rxnorm_map_df, output_table_name, replace=replace_all_mapping_tables)

# COMMAND ----------

# DBTITLE 1,ETL RxNorm / Medication ID Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_rxnorm_medication_id_map_{}'.format(version_number)

# Get RxNorm / Medication ID map
rxnorm_medication_id_map_df = get_rxnorm_medication_id_mapping(
  medication_id_rxnorm_map='hadlock_medication_id_rxnorm_map_{}'.format(version_number),
  med_order_table='hadlock_medication_orders_{}'.format(version_number),
  omop_table='omop_concept_{}'.format(omop_ver))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(rxnorm_medication_id_map_df, output_table_name, replace=replace_all_mapping_tables)

# COMMAND ----------

# DBTITLE 1,ETL Lab ID / LOINC Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_lab_id_loinc_map_{}'.format(version_number)

# Get Lab ID / LOINC map
lab_id_loinc_map_df = get_lab_id_loinc_mapping(
  labs_table='hadlock_procedure_orders_{}'.format(version_number),
  omop_table='omop_concept_{}'.format(omop_ver))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(lab_id_loinc_map_df, output_table_name, replace=replace_all_mapping_tables)

# COMMAND ----------

# DBTITLE 1,ETL LOINC / Lab ID Table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_loinc_lab_id_map_{}'.format(version_number)

# Get LOINC / Lab ID map
loinc_lab_id_map_df = get_loinc_lab_id_mapping(
  lab_id_loinc_map='hadlock_lab_id_loinc_map_{}'.format(version_number),
  labs_table='hadlock_procedure_orders_{}'.format(version_number))

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(loinc_lab_id_map_df, output_table_name, replace=replace_all_mapping_tables)
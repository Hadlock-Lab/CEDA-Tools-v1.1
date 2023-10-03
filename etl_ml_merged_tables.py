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

# DBTITLE 1,ETL diagnoses-by-encounter ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_diagnoses_by_encounter_{}'.format(version_number)

# Specify diagnosis clinical concept labels
condition_cc_list = list(cc_condition_snomed_dictionary.keys())

# Specify table and file locations
encounter_table_name = 'hadlock_encounters_{}'.format(version_number)
clinical_concepts_root = "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/rdp_phi_sandbox/hadlock_common/clinical_concepts/"
clinical_concepts_file = clinical_concepts_root + "condition_diagnosis_id_dictionary_{}.json".format(version_number)

# Get encounter records for cc list
enc_diag_records_df = get_diagnoses_by_encounter(
  encounter_table_name=encounter_table_name,
  cc_dictionary_file=clinical_concepts_file,
  cc_label_list=condition_cc_list)
# Write results to sandbox
write_data_frame_to_sandbox_delta_table(enc_diag_records_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL diagnoses-by-patient ML table
# Specify name of sandbox table to write to
#
output_table_name = 'hadlock_ml_diagnoses_by_patient_{}'.format(version_number)

# Specify diagnosis clinical concept labels
condition_cc_list = list(cc_condition_snomed_dictionary.keys())

# Specify encounter diagnoses ML table location
enc_diagnoses_ml_table_name = 'hadlock_ml_diagnoses_by_encounter_{}'.format(version_number)

# Get encounter diagnosis records by patient
patient_diag_df = get_diagnoses_by_patient(
  enc_diag_ml_table=enc_diagnoses_ml_table_name,
  cc_label_list=condition_cc_list)

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(patient_diag_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL medications-by-encounter ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_medications_by_encounter_{}'.format(version_number)
# Specify medication clinical concept labels
medication_cc_list = list(cc_medication_rxnorm_dictionary.keys())

# Specify table and file locations
med_order_table_name = 'hadlock_medication_orders_{}'.format(version_number)
clinical_concepts_root = "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/rdp_phi_sandbox/hadlock_common/clinical_concepts/"
clinical_concepts_file = clinical_concepts_root + "medication_medication_id_dictionary_{}.json".format(version_number)

# Get medication order records for cc list
enc_med_order_records_df = get_med_orders_by_encounter(
  med_order_table_name=med_order_table_name,
  cc_dictionary_file=clinical_concepts_file,
  cc_label_list=medication_cc_list)
# Write results to sandbox
write_data_frame_to_sandbox_delta_table(enc_med_order_records_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL medications-by-patient ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_medications_by_patient_{}'.format(version_number)

# Specify medication clinical concept labels
medication_cc_list = list(cc_medication_rxnorm_dictionary.keys())

# Specify encounter medications ML table location
enc_medications_ml_table_name = 'hadlock_ml_medications_by_encounter_{}'.format(version_number)

# Get medication records by patient
patient_meds_df = get_med_orders_by_patient(
  enc_meds_ml_table=enc_medications_ml_table_name,
  cc_label_list=medication_cc_list)
# Write results to sandbox
write_data_frame_to_sandbox_delta_table(patient_meds_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL labs-by-encounter ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_labs_by_encounter_{}'.format(version_number)

# Specify lab clinical concept labels
lab_string_search_list = [
  'adenovirus_pcr_naat', 'bordetella_pertussis_pcr_naat', 'clostridium_difficile_pcr_naat', 'coronavirus_other_pcr_naat',
  'herpes_simplex_virus_pcr_naat', 'influenza_pcr_naat', 'influenza_a_pcr_naat', 'influenza_b_pcr_naat', 'sars_cov_2_pcr_naat']
lab_cc_list = list(cc_lab_loinc_dictionary.keys()) + lab_string_search_list

# Specify table and file locations
proc_order_table_name = 'hadlock_procedure_orders_{}'.format(version_number)
clinical_concepts_root = "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/rdp_phi_sandbox/hadlock_common/clinical_concepts/"
clinical_concepts_file = clinical_concepts_root + "lab_lab_id_dictionary_{}.json".format(version_number)

# Get procedure order records for cc list
enc_proc_order_records_df = get_labs_by_encounter(
  proc_order_table_name=proc_order_table_name,
  cc_dictionary_file=clinical_concepts_file,
  cc_label_list=lab_cc_list)

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(enc_proc_order_records_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL labs-by-patient ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_labs_by_patient_{}'.format(version_number)

# Specify lab clinical concept labels
lab_string_search_list = [
  'adenovirus_pcr_naat', 'bordetella_pertussis_pcr_naat', 'clostridium_difficile_pcr_naat', 'coronavirus_other_pcr_naat',
  'herpes_simplex_virus_pcr_naat', 'influenza_pcr_naat', 'influenza_a_pcr_naat', 'influenza_b_pcr_naat', 'sars_cov_2_pcr_naat']
lab_cc_list = list(cc_lab_loinc_dictionary.keys()) + lab_string_search_list

# Specify encounter labs ML table location
enc_labs_ml_table_name = 'hadlock_ml_labs_by_encounter_{}'.format(version_number)

# Get lab records by patient
patient_labs_df = get_labs_by_patient(
  enc_labs_ml_table=enc_labs_ml_table_name,
  cc_label_list=lab_cc_list)

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(patient_labs_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,Combine patient tables and save
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_merged_patient_{}'.format(version_number)
# Specify names of patient ML tables
diag_table_name = 'hadlock_ml_diagnoses_by_patient_{}'.format(version_number)
meds_table_name = 'hadlock_ml_medications_by_patient_{}'.format(version_number)
labs_table_name = 'hadlock_ml_labs_by_patient_{}'.format(version_number)
encounters_table_name = 'hadlock_encounters_{}'.format(version_number)

# Get merged ML patient table
patient_table_df = merge_ml_patient_tables_with_race(
  diag_table_name=diag_table_name,
  meds_table_name=meds_table_name,
  labs_table_name=labs_table_name,
  enc_table_name=encounters_table_name)
# Write results to sandbox
write_data_frame_to_sandbox_delta_table(patient_table_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,Combine encounter tables and save
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_merged_encounter_{}'.format(version_number)

# Specify names of encounter ML tables
diag_table_name = 'hadlock_ml_diagnoses_by_encounter_{}'.format(version_number)
meds_table_name = 'hadlock_ml_medications_by_encounter_{}'.format(version_number)
labs_table_name = 'hadlock_ml_labs_by_encounter_{}'.format(version_number)
encounters_table_name = 'hadlock_encounters_{}'.format(version_number)

# Get merged ML encounter table
encounter_table_df = merge_ml_encounter_tables_with_race(
  diag_table_name=diag_table_name,
  meds_table_name=meds_table_name,
  labs_table_name=labs_table_name,
  enc_table_name=encounters_table_name)

# Write results to sandbox
write_data_frame_to_sandbox_delta_table(encounter_table_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL Translator features-by-encounter ML table
# Specify name of sandbox table to write to
output_table_name = 'hadlock_ml_features_encounter_{}'.format(version_number)

# Specify name of merged encounter ML tables
#'updated_hadlock_ml_merged_encounter'
ml_encounter_table_name = 'hadlock_ml_merged_encounter_{}'.format(version_number)

# Get condition feature definitions
condition_feature_definitions = translator_condition_feature_definitions

## Load medication feature definitions from dictionary directly, instead of from a separate file
## Qi's updated version of code as of 05/31/2023
med_feature_definitions = translator_medication_feature_definitions

# Get lab feature definitions
lab_feature_definitions = translator_lab_feature_definitions

# Generate ML features table by encounter 
ml_features_by_enc_df = generate_ml_features_table_with_race_by_encounter(
  condition_feature_defs=condition_feature_definitions,
  med_feature_defs=med_feature_definitions,
  lab_feature_defs=lab_feature_definitions,
  ml_encounter_table=ml_encounter_table_name)
# Write ML features table to sandbox
write_data_frame_to_sandbox_delta_table(ml_features_by_enc_df, output_table_name, replace=True)

# COMMAND ----------

# DBTITLE 1,ETL Translator features-by-patient ML table
# Specify name of sandbox table to write to
#updated_hadlock_ml_merged_patient'
output_table_name = 'hadlock_ml_features_patient_{}'.format(version_number)
# Specify name of merged patient ML tables
ml_patient_table_name = 'hadlock_ml_merged_patient_{}'.format(version_number)
# Get condition feature definitions
condition_feature_definitions = translator_condition_feature_definitions

## Load medication feature definitions from dictionary directly, instead of from a separate file
## Qi's updated version of code as of 05/31/2023
med_feature_definitions = translator_medication_feature_definitions

# Get lab feature definitions
lab_feature_definitions = translator_lab_feature_definitions

# Generate ML features table by patient
ml_features_by_pat_df = generate_ml_features_table_with_race_by_patient(
  condition_feature_defs=condition_feature_definitions,
  med_feature_defs=med_feature_definitions,
  lab_feature_defs=lab_feature_definitions,
  ml_patient_table=ml_patient_table_name)

# Write ML features table to sandbox
write_data_frame_to_sandbox_delta_table(ml_features_by_pat_df, output_table_name, replace=True)
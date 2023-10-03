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

# DBTITLE 1,Specify folder, file, and table names
# Specify root location for dictionaries
cc_root_path = "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/rdp_phi_sandbox/hadlock_common/"

# Specify clinical concept dictionaries root path
cc_output_folder = cc_root_path + "clinical_concepts/"

# Specify names of dictionary files
condition_diagnosis_id_map_file = "condition_diagnosis_id_dictionary_{}.json".format(version_number)
medication_medication_id_map_file = "medication_medication_id_dictionary_{}.json".format(version_number)
lab_lab_id_map_file = "lab_lab_id_dictionary_{}.json".format(version_number)

# Specify mapping table names
snomed_diagnosis_id_map_table_name = 'hadlock_snomed_diagnosis_id_map_{}'.format(version_number)
rxnorm_medication_id_map_table_name = 'hadlock_rxnorm_medication_id_map_{}'.format(version_number)
loinc_lab_id_map_table_name = 'hadlock_loinc_lab_id_map_{}'.format(version_number)

# COMMAND ----------

# DBTITLE 1,Create location to save dictionaries
# One-time creation of output folder for dictionaries
try:
  dbutils.fs.ls(cc_output_folder)
  print("Output folder already exists:\n" + cc_output_folder)
except:
  dbutils.fs.mkdirs(cc_output_folder)
  print("New output folder created:\n" + cc_output_folder)

# List current contents of output folder for dictionaries
print("\nContents of folder:")
dbutils.fs.ls(cc_output_folder)

# COMMAND ----------

# DBTITLE 1,Optionally delete existing file(s)
## Set to true to delete the existing file
delete_existing_files = True
if (delete_existing_files):
  dbutils.fs.rm(cc_output_folder + condition_diagnosis_id_map_file)
  dbutils.fs.rm(cc_output_folder + medication_medication_id_map_file)
  dbutils.fs.rm(cc_output_folder + lab_lab_id_map_file)
  print("Deleted existing dictionary files.")

# COMMAND ----------

# DBTITLE 1,Generate conditions dictionary once and save
# Generate condition label to diagnosis ID map and save
cc_conditions_dictionary_file = condition_diagnosis_id_map_file
label_to_snomed_dictionary = cc_condition_snomed_dictionary  # Imported from clinical concepts notebook
try:
  dbutils.fs.ls(cc_output_folder + cc_conditions_dictionary_file)
  print("Dictionary '{}' already exists. First delete the existing file to create an updated version.".format(cc_conditions_dictionary_file))
except:
  output_file_name = cc_output_folder + cc_conditions_dictionary_file
  conditions_dictionary = generate_condition_label_to_diagnosis_id_map(
    snomed_diagnosis_id_map_table=snomed_diagnosis_id_map_table_name,
    label_to_snomed_dictionary=label_to_snomed_dictionary)
  print("Saving dictionary '{}'...".format(output_file_name))
  dbutils.fs.put(output_file_name, json.dumps(conditions_dictionary))
  print("\nGenerated new dictionary '{}'.".format(cc_conditions_dictionary_file))

# COMMAND ----------

# DBTITLE 1,Generate medications dictionary once and save
# Generate medication label to medication ID map and save
cc_medications_dictionary_file = medication_medication_id_map_file
label_to_rxnorm_dictionary = cc_medication_rxnorm_dictionary  # Imported from clinical concepts notebook
try:
  dbutils.fs.ls(cc_output_folder + cc_medications_dictionary_file)
  print("Dictionary '{}' already exists. First delete the existing file to create an updated version.".format(cc_medications_dictionary_file))
except:
  output_file_name = cc_output_folder + cc_medications_dictionary_file
  medications_dictionary = generate_medication_label_to_medication_id_map(
    rxnorm_medication_id_map_table=rxnorm_medication_id_map_table_name,
    label_to_rxnorm_dictionary=label_to_rxnorm_dictionary)
  print("Saving dictionary '{}'...".format(output_file_name))
  dbutils.fs.put(output_file_name, json.dumps(medications_dictionary))
  print("\nGenerated new dictionary '{}'.".format(cc_medications_dictionary_file))

# COMMAND ----------

# DBTITLE 1,Generate labs dictionary once and save
# Generate lab label to lab ID map and save
cc_labs_dictionary_file = lab_lab_id_map_file
label_to_loinc_dictionary = cc_lab_loinc_dictionary  # Imported from clinical concepts notebook
try:
  dbutils.fs.ls(cc_output_folder + cc_labs_dictionary_file)
  print("Dictionary '{}' already exists. First delete the existing file to create an updated version.".format(cc_labs_dictionary_file))
except:
  output_file_name = cc_output_folder + cc_labs_dictionary_file
  labs_dictionary = generate_lab_label_to_lab_id_map(
    loinc_lab_id_map_table=loinc_lab_id_map_table_name,
    label_to_loinc_dictionary=label_to_loinc_dictionary)
  print("Saving dictionary '{}'...".format(output_file_name))
  dbutils.fs.put(output_file_name, json.dumps(labs_dictionary))
  print("\nGenerated new dictionary '{}'.".format(cc_labs_dictionary_file))
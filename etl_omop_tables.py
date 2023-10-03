# Databricks notebook source
# DBTITLE 1,Load CEDA ETL Tools
# MAGIC %run
# MAGIC "./load_ceda_etl_tools"

# COMMAND ----------

# DBTITLE 1,Specify folder and table names
## This is the file root path in the old workspace, just here for record
## Specify root location for OMOP files
omop_file_root_path = "abfss://redap-isb-all@stgredapuserrw.dfs.core.windows.net/FileStore/uploads/"

## Specify subfolder for current version of OMOP files
## Previous versions: 
## 2020_08_05/
## 2022_02_11/
## 2022_06_27/
## 2022_11_01/

## Notice: please everytime make sure you get the correct version number!!! Please dont overwrite a older one
current_version = "2022_11_07"

# COMMAND ----------

# DBTITLE 1,Create root folder for OMOP files
# One-time creation of folder for OMOP files
try:
  dbutils.fs.ls(omop_file_root_path)
  print("Root OMOP folder already exists:\n" + omop_file_root_path)
except:
  dbutils.fs.mkdirs(omop_file_root_path)
  print("New root OMOP folder created:\n" + omop_file_root_path)

# List current contents of root OMOP folder
print("\nContents of folder:")
dbutils.fs.ls(omop_file_root_path)

# COMMAND ----------

# DBTITLE 1,Load OMOP files
## Folder location of omop files
## data_folder = omop_file_root_path + omop_current_version_folder
data_folder = omop_file_root_path

## Load concept table
file_name = "CONCEPT_{}.csv".format(current_version)
concept_df = spark.read.format("csv").option("header", "true").option("sep", "\t").load(data_folder+file_name)

## Load concept ancestor table
file_name = "CONCEPT_ANCESTOR_{}.csv".format(current_version)
concept_ancestor_df = spark.read.format("csv").option("header", "true").option("sep", "\t").load(data_folder+file_name)

## Load concept relationship table
file_name = "CONCEPT_RELATIONSHIP_{}.csv".format(current_version)
concept_relationship_df = spark.read.format("csv").option("header", "true").option("sep", "\t").load(data_folder+file_name)

# COMMAND ----------

# DBTITLE 1,QW: Write OMOP table with current version number to sandbox
# Write concept table to sandbox
# default is False
table_name = 'omop_concept_{}'.format(current_version)
write_data_frame_to_sandbox(concept_df, table_name, replace=True)

# Write concept ancestor table to sandbox
table_name = 'omop_concept_ancestor_{}'.format(current_version)
write_data_frame_to_sandbox(concept_ancestor_df, table_name, replace=True)

# Write concept relationship table to sandbox
table_name = 'omop_concept_relationship_{}'.format(current_version)
write_data_frame_to_sandbox(concept_relationship_df, table_name, replace=True)
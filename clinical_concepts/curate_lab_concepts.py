# Databricks notebook source
# DBTITLE 1,Load CEDA Tools
# MAGIC %run
# MAGIC "../load_ceda_api"

# COMMAND ----------

len(lab_loinc_dictionary.keys())

# COMMAND ----------

# DBTITLE 1,Generate list of mapped lab concepts
mapped_loinc_list = [item for k, v in lab_loinc_dictionary.items() for item in v]
mapped_or_excluded_loinc_list = mapped_loinc_list + lab_loinc_exclusion_list
# print(mapped_or_excluded_loinc_list)
print(len(mapped_or_excluded_loinc_list))

# COMMAND ----------

table_name = 'rr_test_hadlock_loinc_lab_id_map'
loinc_lab_id_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))
select_cols = ['loinc', 'count', 'omop_description']
loinc_lab_id_df.select(select_cols).orderBy(F.col('count').desc()).limit(5000).where(~F.col('loinc').isin(mapped_or_excluded_loinc_list)).toPandas()
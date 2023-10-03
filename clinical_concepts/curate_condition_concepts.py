# Databricks notebook source
# DBTITLE 1,Load CEDA Tools
# MAGIC %run
# MAGIC "../load_ceda_api"

# COMMAND ----------

len(cc_condition_snomed_dictionary.keys())

# COMMAND ----------

# DBTITLE 1,Generate list of mapped condition concepts
mapped_snomed_list = [item for k, v in cc_condition_snomed_dictionary.items() for item in v]
mapped_or_excluded_snomed_list = mapped_snomed_list + cc_condition_snomed_exclusion_list
# print(mapped_or_excluded_snomed_list)
print(len(mapped_or_excluded_snomed_list))

# COMMAND ----------

table_name = 'hadlock_snomed_diagnosis_id_map'
snomed_diagnosis_id_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))
select_cols = ['snomed', 'count', 'omop_description']
max_count = 20000; snomed_code_to_find =  24700007
# snomed_diagnosis_id_df \
#   .where(F.col('count') <= max_count) \
#   .select(select_cols) \
#   .orderBy(F.col('count').desc()).limit(10000) \
#   .where(~F.col('snomed').isin(mapped_or_excluded_snomed_list)).toPandas()
snomed_diagnosis_id_df.where(F.col('snomed') == snomed_code_to_find).toPandas()

# COMMAND ----------


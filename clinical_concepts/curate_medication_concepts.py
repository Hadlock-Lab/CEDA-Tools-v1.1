# Databricks notebook source
# DBTITLE 1,Load CEDA Tools
# MAGIC %run
# MAGIC "../load_ceda_api"

# COMMAND ----------

len(cc_medication_rxnorm_dictionary.keys())

# COMMAND ----------

# DBTITLE 1,Generate list of mapped medication concepts
mapped_rxnorm_list = [item for k, v in cc_medication_rxnorm_dictionary.items() for item in v]
mapped_or_excluded_rxnorm_list = mapped_rxnorm_list + cc_medication_rxnorm_exclusion_list
# print(mapped_or_excluded_rxnorm_list)
print(len(mapped_or_excluded_rxnorm_list))

# COMMAND ----------

table_name = 'hadlock_rxnorm_medication_id_map'
rxnorm_medication_id_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))
select_cols = ['rxnorm', 'count', 'short_name']
rxnorm_medication_id_df \
  .select(select_cols) \
  .orderBy(F.col('count').desc()) \
  .limit(10000) \
  .where(~F.col('rxnorm').isin(mapped_or_excluded_rxnorm_list)).toPandas()

#   .where(F.length('rxnorm') <= 5) \

# COMMAND ----------


# Databricks notebook source
# DBTITLE 1,Load CEDA API
# MAGIC %run
# MAGIC "./load_ceda_api"

# COMMAND ----------

# DBTITLE 1,Load CEDA cohort table utilities
# MAGIC %run
# MAGIC "./etl/ceda_cohort_table_utilities"

# COMMAND ----------

# DBTITLE 1,Load CEDA core table utilities
# MAGIC %run
# MAGIC "./etl/ceda_core_table_utilities"

# COMMAND ----------

# DBTITLE 1,Load CEDA dictionary file utilities
# MAGIC %run
# MAGIC "./etl/ceda_dictionary_file_utilities"

# COMMAND ----------

# DBTITLE 1,Load CEDA mapping table utilities
# MAGIC %run
# MAGIC "./etl/ceda_mapping_table_utilities"

# COMMAND ----------

# DBTITLE 1,Load ML core table utilities
# MAGIC %run
# MAGIC "./etl/ml_core_table_utilities"
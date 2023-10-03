# Databricks notebook source
# DBTITLE 1,Load lab clinical concepts
# MAGIC %run
# MAGIC "./lab_concepts"

# COMMAND ----------

# DBTITLE 1,Specify positive and negative result strings
POSITIVE_STRINGS_COMMON = [
  'positive', 'detected', 'presumptive pos', 'detected see scanned report', 'detcted',
  'presumptive positive', 'presumptive positive see scanned report', 'detect', 'inst_positive',
  'added detected', 'positve', 'postive', 'pos']

NEGATIVE_STRINGS_COMMON = [
  'none detected', 'undetected', 'not dectected', 'negative', 'not detected', 'not deteced',
  'not detected see scanned result', 'not detectd', 'not detected see scanned report', 'negatiev',
  'not detected', 'not detected see media', 'non detected', 'not dtected', 'not detected see scanned results',
  'not detecte', 'none detected', 'presumptive negative', 'added none detected', 'not_detect',
  'revised none detected', 'neg', 'negatvie', 'negtive', 'negitive']

POSITIVE_STRINGS_SARS_COV_2 = []

NEGATIVE_STRINGS_SARS_COV_2 = ['prsmptve neg covid']

POSITIVE_STRINGS_INFLUENZA = [
  'positive influenzae a detected', 'flu a positive', 'positive influenzae b detected', 'flu b positive',
  'influenza a rna positive', 'positive influenzae a and 2009 h1n1 detected', 'influenza b rna positive',
  'detected a', 'h1n1 detected']

NEGATIVE_STRINGS_INFLUENZA = [
  'negative no influenzae abdetected', 'flu b negative', 'flu a negative',
  'negative no influenzae ab or 2009 h1n1 detected', 'influenza b rna negative', 'influenza a rna negative',
  'negati', 'negative for a and b']

# COMMAND ----------

# DBTITLE 1,Classify PCR/NAAT results as positive, negative, or other
# Define UDF for mapping PCR/NAAT results to positive/negative/other
def classify_pcr_naat_outcome(result_string, test_type=None):
  # Cleaned strings classified as a positive result
  positive_strings = POSITIVE_STRINGS_COMMON
  
  # Cleaned strings classified as a negative result
  negative_strings = NEGATIVE_STRINGS_COMMON
  
  # Additional strings for specific test
  if (test_type == 'sars_cov_2_pcr_naat'):
    positive_strings = positive_strings + POSITIVE_STRINGS_SARS_COV_2
    negative_strings = negative_strings + NEGATIVE_STRINGS_SARS_COV_2
  if (test_type == 'influenza_pcr_naat'):
    positive_strings = positive_strings + POSITIVE_STRINGS_INFLUENZA
    negative_strings = negative_strings + NEGATIVE_STRINGS_INFLUENZA
  
  # Clean the result value string
  result_cleaned = clean_string(result_string)
  
  # Return mapped string
  if(result_cleaned in positive_strings):
    return 'positive'
  elif(result_cleaned in negative_strings):
    return 'negative'
  else:
    return 'other'
classify_pcr_naat_outcome_udf = F.udf(classify_pcr_naat_outcome, StringType())

# COMMAND ----------


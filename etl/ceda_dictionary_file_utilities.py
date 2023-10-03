# Databricks notebook source
# DBTITLE 1,Define basic utilities
# Define utility function
def get_sorted_unique_list(list_):
  list_ = list(set(list_))
  list_.sort()
  return list_

## Import necessary packages and functions
from pyspark.sql.window import Window
from pyspark.sql.functions import lower, col, lit, when, unix_timestamp, months_between, expr, countDistinct, count, row_number, concat
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, FloatType
from pyspark.sql import SparkSession
from matplotlib import pyplot as plt
from pyspark.ml.feature import Imputer
import numpy as np
import pandas as pd

##################################################################
## Previous omop table versions:
## 2020-08-05, 2022-02-11, 2022-11-01
## Input:
## the code stored in the dictionary based on the current keyword
## Output:
## A dataframe of codes and their corresponding descendant codes
##################################################################

## Notice!! Please always confirm the version of the OMOP table you are using, check the data panel to see if there is a newer version
#print("Please always confirm the version of the OMOP table you are using, check the data panel to see if there is a newer version!!!")

def get_all_descendant_snomed_codes(code, omop_table_version):
  descendant_snomed_codes_df = spark.sql(
  """
  SELECT
    oc1.concept_code as ancestor_snomed_code,
    oc1.concept_name as ancestor_concept_name,
    oc2.concept_code as descendant_snomed_code,
    oc2.concept_name as descendant_concept_name
  FROM (
    SELECT * FROM rdp_phi_sandbox.omop_concept_{version}
    WHERE
      concept_code = {snomed_code} AND
      vocabulary_id = 'SNOMED') as oc1
  JOIN rdp_phi_sandbox.omop_concept_ancestor_{version} as oca
  ON oc1.concept_id = oca.ancestor_concept_id
  JOIN rdp_phi_sandbox.omop_concept_{version} as oc2
  ON oca.descendant_concept_id = oc2.concept_id
  ORDER BY min_levels_of_separation, oc2.concept_name
  """.format(snomed_code=code, version = omop_table_version))
  return descendant_snomed_codes_df

#################################################################################################################################################
## Inputs:
## 2. keyword: the name for the dataframe stored
## 3. keyword_list: the list of diseases cc regitered code name to use
##    check this file for the current registered code names: cmd 183
##    /Users/jennifer.hadlock2@providence.org/GreenerGrass/Jenn - Hadlock Lab Shared/Clinical Concepts/Conditions/conditions_cc_registry
## 4. omop_table_version: the omop_table_version you wished to use, 
## 5. include_descendant_codes: whether to include all descendant SNOMED codes, default is True
##    check this documentation for more: https://docs.google.com/document/d/18rga21a8oacquytfLQBiSwqYXLcy0d-2n4QDL8Qnc6s/edit#heading=h.bi3sttpbfoqr
#################################################################################################################################################
def get_all_snomed_codes_for_condition(keyword, omop_table_version, include_descendant_codes = True):
  full_snomed_list, item = [], keyword
  codes = cc_condition_snomed_dictionary[item]  
  for code in codes:
    temp_df = get_all_descendant_snomed_codes(code, omop_table_version)
    # use toPandas to convert a pyspark dataframe's col into a list (faster than using flatmap)
    cur_snomed_list = list(temp_df.select('descendant_snomed_code').toPandas()['descendant_snomed_code'])
    full_snomed_list += cur_snomed_list
    
  ## Convert to set to make sure no duplicates    
  snomed_codes = list(set(full_snomed_list))
      
  if item in patches_cc_registry:
    list_to_ignore = patches_cc_registry[item]
    ## Following line only used to debug
    #print(list_to_ignore)
    ## Exclude snomed codes from the list_to_ignore
    snomed_codes = [ele for ele in snomed_codes if ele not in list_to_ignore]
  #   print("The special treatment to remove unwanted codes from {} is working!".format(item))
  #   #print("Here is the list of snomed codes found: {}".format(snomed_codes))
  # else:
  #   print("No special treatment to remove codes from {} now, please contact Jenn or the doctor you worked with to confirm.".format(item))
      
  ## Remove all strange strings start with OMOP
  def checkPrefix(x):
    prefix = "OMOP"
    if x.startswith(prefix):
      return False
    else: return True 
  new_snomed_codes = list(filter(checkPrefix, snomed_codes))
  
  ## Convert string
  snomed_codes_int = [eval(i) for i in new_snomed_codes]
  # print("Here is the list of snomed codes found: {}".format(snomed_codes_int))

  return snomed_codes_int

##################################################################
## Previous omop table versions:
## 2020-08-05, 2022_02_11
## Input:
## the code stored in the dictionary based on the current keyword
## Output:
## A dataframe of codes and their corresponding descendant codes
##################################################################
def get_all_descendant_RxNorm_codes(code, omop_table_version):
  descendant_RxNorm_codes_df = spark.sql(
  """
  SELECT
    oc1.concept_code as ancestor_RxNorm_code,
    oc1.concept_name as ancestor_concept_name,
    oc2.concept_code as descendant_RxNorm_code,
    oc2.concept_name as descendant_concept_name
  FROM (
    SELECT * FROM rdp_phi_sandbox.omop_concept_{version}
    WHERE
      concept_code = {RxNorm_code} AND
      vocabulary_id = 'RxNorm') as oc1
  JOIN rdp_phi_sandbox.omop_concept_ancestor_{version} as oca
  ON oc1.concept_id = oca.ancestor_concept_id
  JOIN rdp_phi_sandbox.omop_concept_{version} as oc2
  ON oca.descendant_concept_id = oc2.concept_id
  ORDER BY min_levels_of_separation, oc2.concept_name
  """.format(RxNorm_code=code, version = omop_table_version))
  return descendant_RxNorm_codes_df

#########################################################################################
## Function for searching and storing medication information
## Inputs:
## 1. Add merged patient & medication orders dataframe
## 2. Add keyword_list for medication codes
## 3. Add corresponding possible routes
## 4. Add the name of the current medication
## 5. Add how many days prior will the medication being considered still effective
#########################################################################################
def get_all_rxnorm_codes_for_medication(keyword, omop_table_version):
    
    ## Define a full snomed list to store all descendants found
    full_RxNorm_list, item = [], keyword
    
    codes = cc_medication_rxnorm_dictionary[item]
    for code in codes:
      temp_df = get_all_descendant_RxNorm_codes(code, omop_table_version)
      # use toPandas to convert a pyspark dataframe's col into a list (faster than using flatmap)
      cur_RxNorm_list = list(temp_df.select('descendant_RxNorm_code').toPandas()['descendant_RxNorm_code'])
      full_RxNorm_list += cur_RxNorm_list
      
    if item in patches_cc_registry:
      list_to_ignore = patches_cc_registry[item]
      ## Following line only used to debug
      #print(list_to_ignore)
      ## Exclude snomed codes from the list_to_ignore
      full_RxNorm_list = [ele for ele in full_RxNorm_list if ele not in list_to_ignore]
    #   print("The special treatment to remove unwanted codes from {} is working!".format(item))
    #   #print("Here is the list of snomed codes found: {}".format(snomed_codes))
    # else:
    #   print("No special treatment to remove codes from {} now, please contact Jenn or the doctor you worked with to confirm.".format(item))
      
    ## Convert to set to make sure no duplicates    
    RxNorm_codes = list(set(full_RxNorm_list))
    
    ## Remove all strange strings start with OMOP
    def checkPrefix(x):
      prefix = "OMOP"
      if x.startswith(prefix):
        return False
      else: return True 
    new_RxNorm_codes = list(filter(checkPrefix, RxNorm_codes))
    
    ## Convert string
    rxnorm_codes_int = [eval(i) for i in new_RxNorm_codes]

    ## Temp comment out to speed up
    # print("Here is the list of RxNorm codes found: {}".format(rxnorm_codes_int))
    return rxnorm_codes_int

# COMMAND ----------

# DBTITLE 1,Generate mapping from condition labels to diagnosis IDs
# Generate condition label to diagnosis ID list map
def generate_condition_label_to_diagnosis_id_map(snomed_diagnosis_id_map_table, label_to_snomed_dictionary, omop_table_version = "2022_11_07"):

  # Load SNOMED / diagnosis ID map table
  table_name = snomed_diagnosis_id_map_table
  snomed_map_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))

  # Get conditions list
  conditions_list = list(label_to_snomed_dictionary.keys()); conditions_list.sort()
  ## Notice: original code will only read what is manually inputed in the dictionary, new function will find all descendants
  data_temp = [(c, get_all_snomed_codes_for_condition(c, omop_table_version)) for c in conditions_list]

  # Create clinical concept map dataframe
  columns = ['label', 'snomed_list']
  w = Window.partitionBy('label').orderBy('diagnosis_id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  condition_list_df = spark.createDataFrame(data_temp, columns) \
    .select('label', F.explode('snomed_list').alias('snomed')) \
    .join(snomed_map_df, ['snomed'], how='left') \
    .select('label', 'snomed', F.explode('diagnosis_id').alias('diagnosis_id')).dropDuplicates() \
    .select('label', F.collect_list('diagnosis_id').over(w).alias('diagnosis_id')).dropDuplicates() \
    .orderBy('label')
  condition_list_rdd = condition_list_df.rdd.collect()

  # Generate final condition label to diagnosis IDs map
  condition_diagnosis_id_dictionary = {row.label: get_sorted_unique_list(row.diagnosis_id) for row in condition_list_rdd}
  
  return condition_diagnosis_id_dictionary

# COMMAND ----------

# DBTITLE 1,Generate mapping from medication labels to medication IDs
# Generate medication label to medication ID list map
def generate_medication_label_to_medication_id_map(rxnorm_medication_id_map_table, label_to_rxnorm_dictionary, omop_table_version = "2022_11_07"):

  # Load RxNorm / medication ID map table
  table_name = rxnorm_medication_id_map_table
  rxnorm_map_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))

  # Get medications list
  medications_list = list(label_to_rxnorm_dictionary.keys()); medications_list.sort()
  
  ## Notice: original code will only read what is manually inputed in the dictionary, new function will find all descendants
  data_temp = [(c, get_all_rxnorm_codes_for_medication(c, omop_table_version)) for c in medications_list]

  # Create clinical concept map dataframe
  columns = ['label', 'rxnorm_list']
  w = Window.partitionBy('label').orderBy('medication_id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  medication_list_df = spark.createDataFrame(data_temp, columns) \
    .select('label', F.explode('rxnorm_list').alias('rxnorm')) \
    .join(rxnorm_map_df, ['rxnorm'], how='left') \
    .select('label', 'rxnorm', F.explode('medication_id').alias('medication_id')).dropDuplicates() \
    .select('label', F.collect_list('medication_id').over(w).alias('medication_id')).dropDuplicates() \
    .orderBy('label')
  medication_list_rdd = medication_list_df.rdd.collect()

  # Generate final medication label to medication IDs map
  medication_medication_id_dictionary = {row.label: get_sorted_unique_list(row.medication_id) for row in medication_list_rdd}
  
  return medication_medication_id_dictionary

# COMMAND ----------

# DBTITLE 1,Generate mapping from lab labels to lab IDs
# Generate lab label to lab ID list map
def generate_lab_label_to_lab_id_map(loinc_lab_id_map_table, label_to_loinc_dictionary):

  # Load LOINC / lab ID map table
  table_name = loinc_lab_id_map_table
  loinc_map_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name))

  # Get labs list
  labs_list = list(label_to_loinc_dictionary.keys()); labs_list.sort()
  data_temp = [(c, label_to_loinc_dictionary[c]) for c in labs_list]

  # Create clinical concept map dataframe
  columns = ['label', 'loinc_list']
  w = Window.partitionBy('label').orderBy('lab_id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  lab_list_df = spark.createDataFrame(data_temp, columns) \
    .select('label', F.explode('loinc_list').alias('loinc')) \
    .join(loinc_map_df, ['loinc'], how='left') \
    .select('label', 'loinc', F.explode('lab_id').alias('lab_id')).dropDuplicates() \
    .select('label', F.collect_list('lab_id').over(w).alias('lab_id')).dropDuplicates() \
    .orderBy('label')
  lab_list_rdd = lab_list_df.rdd.collect()

  # Generate final lab label to lab IDs map
  lab_lab_id_dictionary = {row.label: get_sorted_unique_list(row.lab_id) for row in lab_list_rdd}
  
  return lab_lab_id_dictionary
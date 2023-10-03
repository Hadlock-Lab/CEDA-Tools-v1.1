# Databricks notebook source
# DBTITLE 1,Convenience functions to get cc and feature labels
# Specify strings that indicate presence/detection
PRESENCE_STRINGS = ['presence', '_in_', 'appearance', 'color', 'mutation']

# Get list for pos/neg labs
def get_pos_neg_label_list():
  return list(cc_lab_search_strings.keys())

# Get list for high/low labs
def get_high_low_label_list():
  non_pos_neg_label_list = list(cc_lab_loinc_dictionary.keys())
  def is_high_low_label(l): return not(any([(s in l) for s in PRESENCE_STRINGS]))
  return [label for label in non_pos_neg_label_list if is_high_low_label(label)]

# Get labs for presence labs
def get_presence_label_list():
  non_pos_neg_label_list = list(cc_lab_loinc_dictionary.keys())
  def is_presence_label(l): return any([(s in l) for s in PRESENCE_STRINGS])
  return [label for label in non_pos_neg_label_list if is_presence_label(label)]

# Get all lab feature labels
def get_lab_feature_labels():
  pos_neg_features = [item_ for l in get_pos_neg_label_list() for item_ in ['{}_pos'.format(l), '{}_neg'.format(l)]]
  high_low_features = [item_ for l in get_high_low_label_list() for item_ in ['{}_high'.format(l), '{}_low'.format(l)]]
  presence_features = ['{}_abnormal'.format(l) for l in get_presence_label_list()]
  return pos_neg_features + high_low_features + presence_features

# Get all feature labels
def get_all_feature_labels():
  condition_features = list(cc_condition_snomed_dictionary.keys())
  medication_features = list(cc_medication_rxnorm_dictionary.keys())
  lab_features = get_lab_feature_labels()
  return condition_features + medication_features + lab_features

# COMMAND ----------

# DBTITLE 1,Load feature definitions from specified file
def get_feature_definitions_from_file(feature_definition_file):
  
  # Load feature definitions from file
  feature_definitions_df = spark.read.json(feature_definition_file)
  feature_definitions_raw = feature_definitions_df.rdd.collect()[0].asDict()

  # Convert raw feature definitions to properly-formatted dictionary
  feature_definitions = {}
  for k, row in feature_definitions_raw.items():
    vals = {
      'id': row.id,
      'name': row.name,
      'category': row.category,
      'cc_list': row.cc_list
    }
    feature_definitions.update({k: vals})
  
  return feature_definitions

# COMMAND ----------

# DBTITLE 1,Check if labels have invalid characters
def get_labels_with_invalid_characters(cc_label_list):
  
  # Confirm that labels contain appropriate characters
  not_allowed = [
    chr(c) for c in list(range(32, 48)) + list(range(65, 91)) + 
    list(range(91, 95)) + list(range(96, 97)) + list(range(123, 127))]
  
  # Generate list of labels with invalid characters
  labels_with_invalid_characters = [label for label in cc_label_list if any([(c in label) for c in not_allowed])]
  
  return labels_with_invalid_characters

# COMMAND ----------

# DBTITLE 1,Check if labels are valid clinical concepts
def get_labels_not_in_clinical_concepts(cc_label_list):
  
  # Load all clinical concept dictionaries and get labels
  condition_cc_labels = list(cc_condition_snomed_dictionary.keys())
  medication_cc_labels = list(cc_medication_rxnorm_dictionary.keys())
  lab_cc_labels = list(cc_lab_loinc_dictionary.keys())
  lab_string_search_cc_labels = list(cc_lab_search_strings.keys())
  all_cc_labels = condition_cc_labels + medication_cc_labels + lab_cc_labels + lab_string_search_cc_labels
  
  # Generate list of labels not found in clinical concept dictionaries
  labels_not_in_cc_dictionaries = [label for label in cc_label_list if not(label in all_cc_labels)]
  
  return labels_not_in_cc_dictionaries

# COMMAND ----------

# DBTITLE 1,Check if clinical concept labels are valid
# Validate clinical concept labels
def cc_labels_are_valid(cc_label_list):
  
  # Confirm that labels contain appropriate characters
  invalid_labels = get_labels_with_invalid_characters(cc_label_list)
  invalid_characters = len(invalid_labels) > 0
  if (invalid_characters):
    print("One or more clinical concept labels contain invalid characters!")
    print(invalid_labels)
  
  # Confirm that labels are found in clinical concept dictionaries
  invalid_labels = get_labels_not_in_clinical_concepts(cc_label_list)
  invalid_concepts = len(invalid_labels) > 0
  if (invalid_concepts):
    print("One or more clinical concept labels are not found in dictionaries!")
    print(invalid_labels)
    
  return not(invalid_characters) and not(invalid_concepts)

# COMMAND ----------

# DBTITLE 1,Check if feature labels are valid
def get_invalid_feature_labels(feature_label_list):
  
  # Get all valid feature labels
  all_valid_feature_labels = get_all_feature_labels()
  
  # Generate list of labels not found in full list of feature labels
  invalid_feature_labels = [label for label in feature_label_list if not(label in all_valid_feature_labels)]
  
  return invalid_feature_labels

# COMMAND ----------


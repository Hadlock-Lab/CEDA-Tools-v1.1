# Databricks notebook source
# DBTITLE 1,Load CEDA API
# MAGIC %run
# MAGIC "./load_ceda_api"

# COMMAND ----------

# DBTITLE 1,Define utility functions and UDFs
# Utility function to floor datetime to nearest 6 hours
def floor_datetime(dt):
  if (dt is None):
    return None
  
  # If string, convert to datetime
  if (type(dt) == str):
    dt_format = '%Y-%m-%d %H:%M:%S'
    dt = datetime.datetime.strptime(dt, dt_format)
  
  # Floor datetime to nearest 6 hours
  floored_hr = math.floor(dt.hour/6)*6
  dt = dt.replace(hour=floored_hr, minute=0, second=0)
  
  return dt

# Define UDF for floor_datetime
floor_datetime_udf = F.udf(floor_datetime, TimestampType())

# Utility function to get 6-hr datetime steps between two dates (inclusive)
def get_timestamp_array(dt1, dt2):
  # Empty array if there is no start datetime
  if (dt1 is None):
    return []
  
  # First datetime as single item if no stop datetime
  if (dt2 is None):
    return [floor_datetime(dt1)]
  
  # Convert and floor (to nearest 6 hours) both datetimes
  dt1 = floor_datetime(dt1)
  dt2 = floor_datetime(dt2)
  
  # Generate array of datetimes spaced by 6 hours
  dt_array = []
  while (dt1 <= dt2):
    dt_array.append(dt1)
    dt1 = dt1 + datetime.timedelta(hours=6)
  
  return dt_array

# Define UDF for get_timestamp_array
get_timestamp_array_udf = F.udf(get_timestamp_array, ArrayType(TimestampType()))

# COMMAND ----------

# DBTITLE 1,Specify date bounds
# Set date bounds for generating WHO scores
date_bounds = ['2008-01-01', '2023-05-31']

## Set True if you have already updated both the OMOP and core tables needed
## default option is False
replace_all_mapping_tables = True

## Read the current date as a version string
from pyspark.sql.functions import current_date
version_number = str(spark.range(1).withColumn("date",current_date()).select("date").collect()[0][0]).replace('-', '_')
print("The version number added will be {}".format(version_number))

## Or manually load a version number
## Notice then remember to comment out this following part if you don't want the version number to be overwritten
version_number = "2023_05_31"
print("The manual selected version number added will be {}".format(version_number))

# COMMAND ----------

# DBTITLE 1,Get encounter records
# Get encounter records
table_name = 'hadlock_encounters_{}'.format(version_number)
column_selection = ['patient_id', 'encounter_id', 'contact_date', 'discharge_disposition']
encounters_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name)) \
  .where(F.col('contact_date').between(*date_bounds)) \
  .select(*column_selection, 'death_date', 'admission_datetime', 'discharge_datetime',
          get_timestamp_array_udf(F.col('admission_datetime'), F.col('discharge_datetime')).alias('dt_array')) \
  .select(*column_selection,
          floor_datetime_udf('death_date').alias('death_dt'),
          floor_datetime_udf('admission_datetime').alias('admission_dt'),
          floor_datetime_udf('discharge_datetime').alias('discharge_dt'),
          F.explode('dt_array').alias('record_dt')) \
  .dropDuplicates()

# Apply window functions
partition_by_cols = ['patient_id', 'record_dt', 'death_dt']
collect_set_cols = ['encounter_id', 'contact_date', 'admission_dt', 'discharge_dt', 'discharge_disposition']
w = Window.partitionBy(partition_by_cols)
encounters_agg_df = encounters_df \
  .select(*partition_by_cols,
          *[F.collect_set(col).over(w).alias(col) for col in collect_set_cols]) \
  .dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Get device flowsheet records
# Get flowsheet measurement IDs for devices
labels = ['oxygen_device', 'crrt', 'ecmo']
flowsheet_measurement_ids = flowsheet_id_concept_map
flowsheet_measurement_ids = {l: [str(k)+str(id_) for k, v in flowsheet_measurement_ids[l].items() for id_ in v] for l in labels}

# Define UDF for adding T/F column identifying device records
def is_device_record(flowsheet_meas_id, device):
  if (flowsheet_meas_id is None):
    return None
  return str(flowsheet_meas_id) in flowsheet_measurement_ids[device]
is_device_record_udf = F.udf(is_device_record, BooleanType())

# Get flowsheet records
table_name = 'hadlock_flowsheets_{}'.format(version_number)
device_labels = ['oxygen_device', 'crrt', 'ecmo']
column_selection = ['patient_id', 'flowsheet_measurement_id', 'recorded_datetime', 'value', 'name', 'encounter_id']
flowsheets_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name)) \
  .where(F.col('recorded_datetime').between(*date_bounds)) \
  .select(*column_selection, floor_datetime_udf(F.col('recorded_datetime')).alias('record_dt'),
          *[is_device_record_udf(F.col('flowsheet_measurement_id'), F.lit(label)).alias(label) for label in device_labels]) \
  .where(' OR '.join(device_labels))

# Apply window functions
partition_by_cols = ['patient_id', 'record_dt']
collect_set_cols = ['encounter_id', 'flowsheet_measurement_id', 'name', 'value']
aliases = ['device_enc_id', 'flowsheet_meas_id', 'device_name', 'device_value']
max_cols = device_labels
w = Window.partitionBy(partition_by_cols)
flowsheets_agg_df = flowsheets_df \
  .select(*partition_by_cols,
          *[F.collect_set(c).over(w).alias(a) for c, a in zip(collect_set_cols, aliases)],
          *[F.max(col).over(w).alias(col) for col in max_cols]) \
  .dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Get medication administration records
# Specify strings to search in order description
# NOTE: Relying on RxNorm misses an appreciable number of records
vasopressor_rxnorm = [
  '3616',  # dobutamine
  '3628',  # dopamine
  '3992',  # epinephrine
  '6963',  # midodrine
  '7512',  # norepinephrine
  '8163'   # phenylephrine
]
vasopressin_string = 'vasopressin'
med_filter_string = "(rxnorm IN ({}))".format(', '.join(["'{}'".format(c) for c in vasopressor_rxnorm]))
med_filter_string = med_filter_string + " OR (lower(order_description) LIKE '%{}%')".format(vasopressin_string)

# Get RxNorm / medication ID mapping
table_name = 'hadlock_rxnorm_medication_id_map_{}'.format(version_number)
rxnorm_medication_id_map_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name)) \
  .select('rxnorm', F.explode('medication_id').alias('medication_id')).dropDuplicates()

# Get medication order/admin records
table_name = 'hadlock_medication_orders_{}'.format(version_number)
column_selection = ['patient_id', 'med_admin_or_order_dt', 'action_taken', 'order_description', 'encounter_id']
med_admin_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(table_name)) \
  .withColumn('med_admin_or_order_dt',
              F.when(F.col('administration_datetime').isNotNull(),
                     F.col('administration_datetime')).otherwise(F.col('ordering_datetime'))) \
  .where(F.col('med_admin_or_order_dt').between(*date_bounds)) \
  .join(rxnorm_medication_id_map_df, ['medication_id'], how='left') \
  .where(med_filter_string) \
  .select(*column_selection, 'rxnorm', 'medication_id',
          floor_datetime_udf(F.col('med_admin_or_order_dt')).alias('record_dt'))

# Apply window functions
partition_by_cols = ['patient_id', 'record_dt']
collect_set_cols = ['encounter_id', 'rxnorm', 'medication_id', 'order_description', 'action_taken']
aliases = ['med_order_enc_id', 'rxnorm_codes', 'medication_id', 'med_order_description', 'action_taken']
w = Window.partitionBy(partition_by_cols)
med_admin_agg_df = med_admin_df \
  .select(*partition_by_cols,
          *[F.collect_set(c).over(w).alias(a) for c, a in zip(collect_set_cols, aliases)]) \
  .dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Confirm there are no column name conflicts
encounter_cols = set(encounters_agg_df.columns)
flowsheet_cols = set(flowsheets_agg_df.columns)
med_admin_cols = set(med_admin_agg_df.columns)
assert(encounter_cols.intersection(flowsheet_cols) == {'record_dt', 'patient_id'})
assert(encounter_cols.intersection(flowsheet_cols) == encounter_cols.intersection(med_admin_cols))
assert(encounter_cols.intersection(med_admin_cols) == flowsheet_cols.intersection(med_admin_cols))

# COMMAND ----------

# DBTITLE 1,Define function to calculate WHO score
def get_who_score(record_dt, death_dt, admit_dt, disch_dt, disch_disp, oxygen, crrt, ecmo, device_value, med_action_taken):
  # Specify oxygen device strings
  device_string_map = {
    6: ['mechanical ventilation', 'manual bag ventilation', 'ventilator'],
    5: ['high-flow nasal cannula', 'hfnc', 'non-invasive ventilation', 'cpap', 'bi-pap', 'bipap', 'high frequency ventilation', 'et tube', 'ett', 't-piece'],
    4: ['nasal cannula', 'simple face mask', 'open oxygen mask', 'tracheostomy collar', 'nonrebreather mask', 'non-rebreather mask', 'nrb mask', 'cannula', 'mask-simple', 'o2/cannula', 'o2/simple mask', 'simple mask', 'mask-nrb', 'mask-aerosol', 'continuous aerosol', 'oxymask', 'o2 via face mask', 'venti-mask', 'vapotherm', 'laryngeal mask airway', 'aerosol mask', 'vapotherm', 'face tent', 'oxygen tent'],
    3: ['room air']
  }
  
  # Check if patient is expired
  if (not(death_dt is None)):
    if (record_dt >= death_dt):
      return 8
  elif (not(disch_dt is None) and (len(disch_dt) > 0)):
    if (not(disch_disp is None) and ('Expired' in disch_disp)):
      if (record_dt >= max(disch_dt)):
        return 8
  
  # Missing oxygen record
  if ((oxygen is None) or (oxygen == False)):
    # Implement logic for assignments based on admit and discharge datetimes
    if (not(admit_dt is None) and (len(admit_dt) > 0)):
      if (not(disch_dt is None) and (len(disch_dt) > 0)):
        # 3 if record datetime is on or after admit and before discharge datetime
        if ((record_dt >= min(admit_dt)) and (record_dt < max(disch_dt))):
          return 3
        # 2 if on discharge datetime
        elif (record_dt == max(disch_dt)):
          return 2
      else:
        # 3 if record datetime coincides with admit datetime
        if (record_dt == min(admit_dt)):
          return 3
    else:
      # Cannot determine score without admit and discharge datetimes
      return None
  
  # Get nominal WHO score based on oxygen device
  who_score = None
  if (not(device_value is None)):
    oxygen_device_vals = ';'.join([s.lower() for s in device_value])
    for i in [6, 5, 4, 3]:
      if (any([(s in oxygen_device_vals) for s in device_string_map[i]])):
        who_score = i
        break
    # If 3 and record datetime coincides with discharge, set to 2
    if (who_score == 3):
      if (not(disch_dt is None) and (len(disch_dt) > 0)):
        if (record_dt == max(disch_dt)):
          who_score = 2
  
  # If 6 and additional organ support, adjust up to 7
  if (who_score == 6):
    # CRRT or ECMO
    if ((crrt == True) or (ecmo == True)):
      who_score = 7
    # Vasopressor
    elif (not(med_action_taken is None) and (len(med_action_taken) > 0)):
      actions = ['Rate Change', 'New Bag', 'Given', 'Restarted', 'Rate Verify']
      if (any([(s in med_action_taken) for s in actions])):
        who_score = 7
  
  return who_score

# Create UDF
get_who_score_udf = F.udf(get_who_score, IntegerType())

# COMMAND ----------

# DBTITLE 1,Join dataframes and compute WHO scores
# Get full set of patients and record datetimes
join_cols = ['patient_id', 'record_dt']
select_cols_1 = ['death_dt', 'encounter_id', 'contact_date', 'admission_dt', 'discharge_dt', 'discharge_disposition']
who_arg_cols = ['record_dt', 'death_dt', 'admission_dt', 'discharge_dt', 'discharge_disposition', 'oxygen_device', 'crrt', 'ecmo', 'device_value', 'action_taken']
select_cols_2 = ['oxygen_device', 'crrt', 'ecmo', 'device_enc_id', 'device_name', 'device_value', 'med_order_enc_id', 'rxnorm_codes', 'med_order_description', 'action_taken']

who_scores_df = encounters_agg_df \
  .join(flowsheets_agg_df, join_cols, how='full') \
  .join(med_admin_agg_df, join_cols, how='full') \
  .select(*join_cols, *select_cols_1,
          get_who_score_udf(*[F.col(c) for c in who_arg_cols]).alias('who_score'),
          *select_cols_2)

# COMMAND ----------

# DBTITLE 1,Write final dataframe to sandbox
# Specify output table
output_table_name = 'hadlock_who_scores_{}'.format(version_number)

# Save table
write_data_frame_to_sandbox_delta_table(who_scores_df, output_table_name, replace=True)
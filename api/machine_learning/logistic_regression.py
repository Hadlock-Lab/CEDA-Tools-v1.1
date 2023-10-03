# Databricks notebook source
# DBTITLE 1,Load needed packages
import statsmodels.api as sm
import statsmodels
import lxml

# COMMAND ----------

# DBTITLE 1,Run Logistic Regression ML Model
# Define function for running logistic regression
def run_logistic_regression(ml_encounter_table, outcome_column):

  # Load ML encounter table and select patients >= 18 years of age
  ml_encounters_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(ml_encounter_table))

  # Specify non-clinical-concept and clinical concept columns
  non_cc_col_names = ['patient_id', 'age', 'sex', 'ethnic_group', 'encounter_id', 'first_record', 'last_record', 'race']
  feature_col_names = [c for c in ml_encounters_df.columns if not(c in non_cc_col_names)]

  # Add outcome column and demographics feature columns
  outcome_label = outcome_column
  w1 = Window.partitionBy('patient_id')
  ml_encounters_df = ml_encounters_df.withColumn('outcome', F.max(outcome_label).over(w1))

  # Get positive encounters and select only the last (chronologically) for each patient
  w2 = Window.partitionBy('patient_id').orderBy('first_record', 'last_record') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  ml_enc_pos_df = ml_encounters_df \
    .where(F.col(outcome_label) == 1) \
    .withColumn('latest_encounter_id', F.max('encounter_id').over(w2)) \
    .where(F.col('latest_encounter_id') == F.col('encounter_id'))

  # Get negative encounters and select only the last (chronologically) for each patient
  ml_enc_neg_df = ml_encounters_df \
    .where(F.col('outcome') == 0) \
    .withColumn('latest_encounter_id', F.max('encounter_id').over(w2)) \
    .where(F.col('latest_encounter_id') == F.col('encounter_id'))
  
  # Get positive and negative patient counts
  pos_patient_count = ml_enc_pos_df.count()
  neg_patient_count = ml_enc_neg_df.count()
  
  ## Take union of positive and negative records and select feature and outcome columns
  ##demographic_features = ['age_gte_65', 'male_sex', 'hispanic']
  final_feature_col_names = [c for c in feature_col_names if not(c in [outcome_label])]
  ml_data_df = ml_enc_pos_df.union(ml_enc_neg_df) \
    .select('outcome', *final_feature_col_names)

  ## Remove pyspark dataframe once no longer need them
  ml_encounters_df.unpersist()
  ml_enc_pos_df.unpersist()
  ml_enc_neg_df.unpersist()

  ################################################################################################
  ## Added the filtering part by Qi WEI
  ###################################################
  ## To filter out 0 samples for a certain feature on an outcome
  ## Limit to only patient with outcome == 1
  ml_pos_data_df = ml_data_df.filter(ml_data_df.outcome == 1)
  ### Convert to pandas dataframe and get the sum of each cols
  ## Notice: this part has potential OOM issue, could potentially solved by shorten the list of factors
  ## Maybe rewrite this whole part in pyspark in order to avoid this issue?
  ## Potential fix: to use to_pandas_on_spark & pandas.api wont satisfy statsmodel
  test_df = ml_pos_data_df.toPandas()
  sum_df = test_df.sum(axis=0)
  ## Convert from series to dataframe
  sum_pd_df = sum_df.to_frame().reset_index()
  sum_pd_df.columns = ["name", "sum"]
  feature_to_remove_df = sum_pd_df[sum_pd_df['sum'] < 10]
  ## Using Series.values.tolist() to get the list of features which need to be removed
  feature_to_remove_list = feature_to_remove_df.name.values.tolist()

  ## Remove dataframes once no longer need them
  ml_pos_data_df.unpersist()
  del test_df
  del sum_df
  del sum_pd_df

  ## List of feature to remove
  ml_data_df = ml_data_df.drop(*feature_to_remove_list)
  final_feature_col_names = list(set(final_feature_col_names) - set(feature_to_remove_list))
  ##############################################################################################

  #########################################################
  ## Added the statsmodel part by Qi WEI
  #######################################################
  
  ## Set LR optimizer to be
  ## Options: lbfgs, powell, cg, ncg, bfgs, basinhopping, newton, ‘nm’ for Nelder-Mead
  ## Notice: Need to discuss which optimizer to be used
  select_method = 'lbfgs'

  ## Prepare train and response dataset
  ## Notice: toPandas in this part has potential OOM issue, could potentially solved by shorten the list of factors
  ## Potential fix: to use to_pandas_on_spark & pandas.api wont satisfy statsmodel
  ml_data_pd_df = ml_data_df.toPandas()
  ## separate X and Y
  Y_cols = 'outcome'
  train_df_Y = ml_data_pd_df[Y_cols]
  train_df_X = ml_data_pd_df.drop(Y_cols, axis=1)

  LR_statmodel = sm.Logit(train_df_Y, train_df_X, missing = 'drop').fit(method = select_method, skip_hessian = False)

  ## Acquire LR summary
  os_results_summary = LR_statmodel.summary()
  ## Note that tables is a list. The table at index 1 is the "core" table. Additionally, read_html puts dfs in a list, so we want index 0
  os_results_as_html = os_results_summary.tables[1].as_html()
  os_odds_ratio_df = pd.read_html(os_results_as_html, header=0, index_col=0)[0]

  ## get the CI interval
  os_odds_ratio_df = os_odds_ratio_df[["coef", "[0.025", "0.975]", "P>|z|"]]
  os_odds_ratio_df.rename(columns={'coef': 'log_odds_ratio', '[0.025': 'lower', '0.975]':'upper', 'P>|z|': 'p_value'}, inplace=True)

  ## Get those individual vectors of needed cols
  log_odds_ratio, lower, upper, p_value = list(os_odds_ratio_df["log_odds_ratio"]), list(os_odds_ratio_df["lower"]), list(os_odds_ratio_df["upper"]), list(os_odds_ratio_df["p_value"])
  ########################################################################################################################################################################################
  
  ## Assemble feature values and add to stages list
  assembler = VectorAssembler(inputCols=final_feature_col_names, outputCol='all_features')
  stages = [assembler]
  
  ## Create pipeline model
  pipeline = Pipeline(stages = stages)
  pipelineModel = pipeline.fit(ml_data_df)
  model_df = pipelineModel.transform(ml_data_df)
  model_df = model_df.select('outcome', 'all_features')

  ## Remove dataframes once no longer need them
  ml_data_df.unpersist()
  del ml_data_pd_df
  del train_df_Y
  del train_df_X
  del os_odds_ratio_df
  
  ## Add weight column
  model_df = model_df \
    .withColumn('class_weight', F.when(F.col('outcome') == 1, 0.99).otherwise(0.01))
  
  ## Run logistic regression model
  lr = LogisticRegression(
    featuresCol='all_features',
    labelCol='outcome',
    weightCol='class_weight',
    maxIter=50)
  lr_model = lr.fit(model_df)

  ## Remove dataframes once no longer need them
  model_df.unpersist()

  ## Get the area under the ROC
  area_under_roc = lr_model.summary.areaUnderROC

  ###########################################
  ## Modified by Qi WEI
  ##########################################
  ## Store model information in a dictionary
  model_info = {
  'pos_count': pos_patient_count,
  'neg_count': neg_patient_count,
  'feature_labels': final_feature_col_names,
  'log_odds_ratio': log_odds_ratio,
  'lower': lower,
  'upper': upper,
  'p_value': p_value,
  'auc_roc': area_under_roc}

  return lr_model, model_info, lr_model.summary

# COMMAND ----------

# DBTITLE 1,Save model and information to output folder
def save_lr_model_and_info(lr_model, model_info, outcome_label, folder_path):
  
  # Write model information to json file
  info_file_name = '{}_lr_model_info.json'.format(outcome_label)
  full_file_path = folder_path + info_file_name
  try:
    dbutils.fs.ls(full_file_path)
    print("File exists! Deleting existing file...")
    dbutils.fs.rm(full_file_path)
  except:
    pass
  print("Writing model information: '{}'...".format(full_file_path))
  dbutils.fs.put(full_file_path, json.dumps(model_info))

# COMMAND ----------

# DBTITLE 1,Load model and information (og, ceda 1.0 version)
def load_lr_model_and_info(outcome_label, folder_path):
  
  ## Load additional model information
  info_file_name = '{}_lr_model_info.json'.format(outcome_label)
  model_info_df = spark.read.json(folder_path + info_file_name)
  model_info = model_info_df.rdd.collect()[0].asDict()
  
  ## Get model information
  pos_count = model_info['pos_count']
  neg_count = model_info['neg_count']
  feature_list = model_info['feature_labels']
  p_value = model_info['p_value']
  log_odds_ratio = model_info['log_odds_ratio']
  lower = model_info['lower']
  upper = model_info['upper']
  auc_roc = model_info['auc_roc']

  ## Generate table with model coefficients
  coefs_list = [ (feature, float(log_odds_ratio), float(lower), float(upper), outcome_label, pos_count, neg_count, float(p_value),  auc_roc) for feature, log_odds_ratio, lower, upper, p_value in zip(feature_list, log_odds_ratio, lower, upper, p_value)]
  headers = ['feature', 'model_coefficient', 'lower_CI', 'upper_CI', 'outcome', 'positive_patient_count', 'negative_patient_count', 'p_value', 'auc_roc']
  coefs_df = spark.createDataFrame(coefs_list, headers).orderBy(F.col('model_coefficient').desc())

  ## Beta test outputs, original outputs: lr_model, model_info, coefs_df
  ## The outputs lr_model and model_info are not used in any downstream codes? There is no need to actually output them
  return coefs_df

# COMMAND ----------


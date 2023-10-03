# Databricks notebook source
# DBTITLE 1,Load general libraries
import re, json
import requests
import numpy as np
import pandas as pd
import datetime, dateutil
import random, math, statistics
import matplotlib.pyplot as plt

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Load ML Libraries
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, ChiSqSelector
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel

# COMMAND ----------

# DBTITLE 1,Load general utilities
# MAGIC %run
# MAGIC "./api/general_utilities"

# COMMAND ----------

# DBTITLE 1,Load ReDaP utilities
# MAGIC %run
# MAGIC "./api/redap_utilities"

# COMMAND ----------

# DBTITLE 1,Load general clinical concept utilities
# MAGIC %run
# MAGIC "./clinical_concepts/general_utilities"

# COMMAND ----------

# DBTITLE 1,Load condition concept utilities
# MAGIC %run
# MAGIC "./clinical_concepts/condition_utilities"

# COMMAND ----------

# DBTITLE 1,Load medication concept utilities
# MAGIC %run
# MAGIC "./clinical_concepts/medication_utilities"

# COMMAND ----------

# DBTITLE 1,Load lab concept utilities
# MAGIC %run
# MAGIC "./clinical_concepts/lab_utilities"

# COMMAND ----------

# DBTITLE 1,Load flowsheet concept utilities
# MAGIC %run
# MAGIC "./clinical_concepts/flowsheet_utilities"

# COMMAND ----------

# DBTITLE 1,Load clinical concept name resolution utilities
# MAGIC %run
# MAGIC "./clinical_concepts/concept_label_id_resolution"

# COMMAND ----------

# DBTITLE 1,Load patches utilities
# MAGIC %run
# MAGIC "./clinical_concepts/patches_utilities"

# COMMAND ----------

# DBTITLE 1,Load ML utilities
# MAGIC %run
# MAGIC "./api/machine_learning/logistic_regression"
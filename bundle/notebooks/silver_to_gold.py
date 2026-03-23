# Databricks notebook source
# Magic # 

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver to Gold Notebook

# COMMAND ----------
from pyspark.sql import functions as psf
from pyspark.sql.window import Window

import sys
from pathlib import Path
import os

# COMMAND ----------

try:
    dbutils
except NameError:
    # pyspark.dbutils exists only in a running cluster; static analyzers or
    # local Python interpreters may not have it, so import inside try/except.
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
    except ImportError:
        DBUtils = None
    if DBUtils is not None:
        dbutils = DBUtils(spark)  # type: ignore
    else:
        pass

# COMMAND ----------

bundle_root_path = dbutils.widgets.get("bundle_root_path")
sys.path.append(os.path.dirname(bundle_root_path))

from bundle.src.silver_gold import SilverToGold

config_path = f"{bundle_root_path}/configs/env_config.yaml"

gold_transformer = SilverToGold(bundle_root_path, dbutils, config_path)
gold_transformer.run()
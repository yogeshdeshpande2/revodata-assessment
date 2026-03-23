# Databricks notebook source
# Magic # 


# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze to Silver Notebook

# COMMAND ----------

# import requests
# import json
from pyspark.sql import functions as psf
from pyspark.sql.types import ArrayType

import sys
import os
from pathlib import Path

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

from bundle.src.bronze_silver import BronzeToSilver

config_path = f"{bundle_root_path}/configs/env_config.yaml"
transformer = BronzeToSilver(bundle_root_path, dbutils, config_path)
transformer.run()
# Databricks notebook source
# Magic #

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion Notebook for Raw to Bronze Layer
# MAGIC This notebook reads raw JSON & CSV files from ADLS, creates bronze tables in Unity Catalog, and writes the raw data into those tables while preserving the nested schema.

# COMMAND ----------


import sys
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
        from bundle.utils import Utilities

        spark = Utilities.get_spark()
        dbutils = DBUtils(spark)  # type: ignore
    else:
        pass

# COMMAND ----------

# Retrieve the bundle root path from the widget and add it to the system path for module imports
bundle_root_path = dbutils.widgets.get("bundle_root_path")
sys.path.append(os.path.dirname(bundle_root_path))

# ruff: noqa: E402
from bundle.src.raw_bronze import RawToBronze

# Set the path to the configuration file and initialize the transformer
config_path = f"{bundle_root_path}/configs/env_config.yaml"
transformer = RawToBronze(bundle_root_path, dbutils, config_path)
transformer.run()

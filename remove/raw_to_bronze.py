# Databricks notebook source
# Magic #

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion Notebook for Raw to Bronze Layer
# MAGIC This notebook reads raw JSON files from ADLS, creates bronze tables in Unity Catalog, and writes the raw data into those tables while preserving the nested schema.

# COMMAND ----------

import requests
import json
import sys
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
sys.path.append(bundle_root_path)

from src.raw_to_bronze import RawToBronze

config_path = f"{bundle_root_path}/configs/env_config.yaml"
transformer = RawToBronze(bundle_root_path, dbutils, config_path)
transformer.run()

# from utils import Utilities
# from utils.config_loader import load_config
# from utils.databricks_logger import DatabricksLogger

# spark = Utilities.get_spark()

# # COMMAND ----------

# # Load config based on environment
# env = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "t"
# config = load_config(f"{bundle_root_path}/configs/env_config.yaml", env=env)
# catalog_name = config["catalog_name"]
# bronze_schema_name = config["bronze_schema_name"]

# raw_airbnb_adls_path = config["raw_airbnb_adls_path"]
# raw_rentals_adls_path = config["raw_rentals_adls_path"]
# raw_postcode_adls_path = config["raw_postcode_adls_path"]

# airbnb_file_name = config["airbnb_file_name"]
# rentals_file_name = config["rentals_file_name"]
# postcode_file_name = config["postcode_file_name"]

# # raw_adls_path = config["raw_adls_path"]
# bronze_adls_path = config["bronze_adls_path"]


# skew_handle_salt_count = config.get("skew_handle_salt_count", 10)  # Default to 10 if not specified

# DatabricksLogger.info(
#     "Configuration loaded successfully",
#     bundle_root_path=bundle_root_path,
#     environment=env,
#     catalog_name=catalog_name,
#     bronze_schema_name=bronze_schema_name,
#     raw_airbnb_adls_path=raw_airbnb_adls_path,
#     raw_rentals_adls_path=raw_rentals_adls_path,
#     raw_postcode_adls_path=raw_postcode_adls_path,
#     airbnb_file_name=airbnb_file_name,
#     rentals_file_name=rentals_file_name,
#     postcode_file_name=postcode_file_name,
#     bronze_adls_path=bronze_adls_path,
#     skew_handle_salt_count=skew_handle_salt_count
# )

# # COMMAND ----------

# bronze_airbnb_table = config["bronze_airbnb_table"]
# bronze_rentals_table = config["bronze_rentals_table"]
# bronze_postcode_table = config["bronze_postcode_table"]

# DatabricksLogger.info(
#     "Bronze table names loaded from config",
#     bronze_airbnb_table=bronze_airbnb_table,
#     bronze_rentals_table=bronze_rentals_table,
#     bronze_postcode_table=bronze_postcode_table
# )

# # COMMAND----------

# # Define raw input paths based on bronze_adls_path
# airbnb_raw_path = raw_airbnb_adls_path.rstrip("/") + f"/{airbnb_file_name}"
# rentals_raw_path = raw_rentals_adls_path.rstrip("/") + f"/{rentals_file_name}"
# postcode_raw_path = raw_postcode_adls_path.rstrip("/") + f"/{postcode_file_name}"

# DatabricksLogger.info(
#     "Defined raw data paths",
#     airbnb_raw_path=airbnb_raw_path,
#     rentals_raw_path=rentals_raw_path,
#     postcode_raw_path=postcode_raw_path
# )

# # COMMAND ----------

# bronze_airbnb_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_airbnb_table}"  # Update catalog.schema as needed
# bronze_rentals_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_rentals_table}"  # Update catalog.schema as needed
# bronze_postcode_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_postcode_table}"  # Update catalog.schema as needed

# DatabricksLogger.info(
#     "Constructed bronze table names",
#     bronze_airbnb_table_name=bronze_airbnb_table_name,
#     bronze_rentals_table_name=bronze_rentals_table_name,
#     bronze_postcode_table_name=bronze_postcode_table_name
# )


# # COMMAND ----------

# renals_df = spark.read.json(rentals_raw_path)

# # Before writing final, check best partitioning column for shows_cleaned_df to handle skew. 
# # column status has cardinality as 3 with reason as: Low cardinality, ideal partition key. Queries filtering by Running/Ended/etc. skip entire partitions.

# # Write to bronze table preserving nested schema
# renals_df.write.mode("overwrite").saveAsTable(bronze_rentals_table_name)
# # spark.sql(f"OPTIMIZE {bronze_rentals_table_name} ZORDER BY (status)")

# DatabricksLogger.info(
#     f"Bronze table '{bronze_rentals_table_name}' created",
#     row_count=renals_df.count(),
#     column_count=len(renals_df.columns)
# )

# # COMMAND ----------

# airbnb_df = spark.read.format("csv").option("header", "true").load(airbnb_raw_path)

# # Before writing final, check best partitioning column for episodes_df to handle skew. 
# # column show_id has cardinality as 10 with reason as: Natural grouping episodes belong to a show. Joins with silver_shows benefit from co-located data.

# # Write to bronze table preserving nested schema
# airbnb_df.write.mode("overwrite").saveAsTable(bronze_airbnb_table_name)
# # spark.sql(f"OPTIMIZE {bronze_airbnb_table_name} ZORDER BY (show_id)")

# DatabricksLogger.info(
#     f"Bronze table '{bronze_airbnb_table_name}' created",
#     row_count=airbnb_df.count(),
#     column_count=len(airbnb_df.columns)
# )

# # COMMAND ----------


# # COMMAND ----------

# DatabricksLogger.info(
#     "Raw to Bronze transformation completed successfully",
#     airbnb_bronze_table=bronze_airbnb_table_name,
#     rentals_bronze_table=bronze_rentals_table_name
# )

# # Command ----------

# renals_df = spark.read.json(rentals_raw_path)

# # Before writing final, check best partitioning column for shows_cleaned_df to handle skew. 
# # column status has cardinality as 3 with reason as: Low cardinality, ideal partition key. Queries filtering by Running/Ended/etc. skip entire partitions.

# # Write to bronze table preserving nested schema
# renals_df.write.mode("overwrite").saveAsTable(bronze_rentals_table_name)
# # spark.sql(f"OPTIMIZE {bronze_rentals_table_name} ZORDER BY (status)")

# DatabricksLogger.info(
#     f"Bronze table '{bronze_rentals_table_name}' created",
#     row_count=renals_df.count(),
#     column_count=len(renals_df.columns)
# )

# # COMMAND ----------

# # airbnb_df = spark.read.format("csv").option("header", "true").load(airbnb_raw_path)
# postcode_df = spark.read.option("multiline", "true").json(postcode_raw_path)

# # Before writing final, check best partitioning column for episodes_df to handle skew. 
# # column show_id has cardinality as 10 with reason as: Natural grouping episodes belong to a show. Joins with silver_shows benefit from co-located data.

# # Write to bronze table preserving nested schema
# postcode_df.write.mode("overwrite").saveAsTable(bronze_postcode_table_name)


# DatabricksLogger.info(
#     f"Bronze table '{bronze_postcode_table_name}' created",
#     row_count=postcode_df.count(),
#     column_count=len(postcode_df.columns)
# )

# # COMMAND ----------


# # COMMAND ----------

# DatabricksLogger.info(
#     "Raw to Bronze transformation completed successfully",
#     airbnb_bronze_table=bronze_airbnb_table_name,
#     rentals_bronze_table=bronze_rentals_table_name,
#     postcode_bronze_table=bronze_postcode_table_name

# )
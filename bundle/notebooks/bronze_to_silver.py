# Databricks notebook source
# Magic # 


# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion Notebook

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import col, explode_outer

import sys
from pathlib import Path

# COMMAND ----------

# ensure a SparkSession is available when running as a script (outside of
# an interactive Databricks notebook where `spark` is injected automatically)
# try:
#     spark
# except NameError:
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.getOrCreate()

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

from utils import Utilities
from utils.config_loader import load_config
from utils.databricks_logger import DatabricksLogger

spark = Utilities.get_spark()

# COMMAND ----------

# Load config based on environment
env = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "t"
config = load_config(f"{bundle_root_path}/configs/env_config.yaml", env=env)
catalog_name = config["catalog_name"]
bronze_schema_name = config["bronze_schema_name"]
silver_schema_name = config["silver_schema_name"]

bronze_adls_path = config["bronze_adls_path"]
silver_adls_path = config["silver_adls_path"]

bronze_shows_table = config["bronze_shows_table"]
bronze_episodes_table = config["bronze_episodes_table"]
bronze_cast_table = config["bronze_cast_table"]

silver_shows_table = config["silver_shows_table"]
silver_episodes_table = config["silver_episodes_table"]
silver_cast_table = config["silver_cast_table"]

skew_handle_salt_count = config.get("skew_handle_salt_count", 10)  # Default to 10 if not specified

DatabricksLogger.info(
    "Configuration loaded successfully",
    bundle_root_path=bundle_root_path,
    environment=env,
    catalog_name=catalog_name,
    bronze_schema_name=bronze_schema_name,
    silver_schema_name=silver_schema_name,
    bronze_adls_path=bronze_adls_path,
    silver_adls_path=silver_adls_path,
    bronze_shows_table=bronze_shows_table,
    bronze_episodes_table=bronze_episodes_table,
    bronze_cast_table=bronze_cast_table,
    silver_shows_table=silver_shows_table,
    silver_episodes_table=silver_episodes_table,
    silver_cast_table=silver_cast_table,
    skew_handle_salt_count=skew_handle_salt_count
)

# COMMAND ----------

shows_bronze_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_shows_table}"
cast_bronze_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_cast_table}"
episodes_bronze_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_episodes_table}"

shows_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_shows_table}"
cast_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_cast_table}"
episodes_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_episodes_table}"

DatabricksLogger.info(
    "Constructed fully qualified table names",
    shows_bronze_table_name=shows_bronze_table_name,
    cast_bronze_table_name=cast_bronze_table_name,
    episodes_bronze_table_name=episodes_bronze_table_name,
    shows_silver_table_name=shows_silver_table_name,
    cast_silver_table_name=cast_silver_table_name,
    episodes_silver_table_name=episodes_silver_table_name
)

# COMMAND ----------

# Read bronze table and flatten
df_bronze_shows = spark.read.table(shows_bronze_table_name)
shows_flatten_df = df_bronze_shows.select(Utilities.flatten_df(df_bronze_shows.schema))
shows_flatten_df = shows_flatten_df.withColumn("genre", explode_outer(col("genres"))).drop("genres")
shows_flatten_df = shows_flatten_df.withColumn("schedule_day", explode_outer(col("schedule_days"))).drop("schedule_days")
shows_cleaned_df = Utilities.handle_null_stringcols(shows_flatten_df)

# Before writing final, check best partitioning column for shows_cleaned_df to handle skew. 
# column status has cardinality as 3 with reason as: Low cardinality, ideal partition key. Queries filtering by Running/Ended/etc. skip entire partitions.

# Write to silver table
shows_cleaned_df.write.mode("overwrite").partitionBy("status").option("mergeSchema", "true").saveAsTable(shows_silver_table_name)
# spark.sql(f"OPTIMIZE {shows_silver_table_name} ZORDER BY (status)")

DatabricksLogger.info(
    f"Silver table '{shows_silver_table_name}' created",
    row_count=shows_cleaned_df.count(),
    column_count=len(shows_cleaned_df.columns)
)

# COMMAND ----------

# Read bronze table and flatten using the same recursive function
df_bronze_cast = spark.read.table(cast_bronze_table_name)
cast_flatten_df = df_bronze_cast.select(Utilities.flatten_df(df_bronze_cast.schema))
cast_cleaned_df = Utilities.handle_null_stringcols(cast_flatten_df)

# Before writing final, check best partitioning column for shows_cleaned_df to handle skew. 
# column show_id has cardinality as 10 with reason as: Natural grouping for cast-per-show lookups and joins with silver_shows.

# Write to silver table
cast_cleaned_df.write.mode("overwrite").partitionBy("show_id").option("mergeSchema", "true").saveAsTable(cast_silver_table_name)
# spark.sql(f"OPTIMIZE {cast_silver_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    f"Silver table '{cast_silver_table_name}' created",
    row_count=cast_cleaned_df.count(),
    column_count=len(cast_cleaned_df.columns)
)

# COMMAND ----------

# Read bronze table and flatten using the same recursive function
df_bronze_episodes = spark.read.table(episodes_bronze_table_name)
episodes_flatten_df = df_bronze_episodes.select(Utilities.flatten_df(df_bronze_episodes.schema))
episodes_cleaned_df = Utilities.handle_null_stringcols(episodes_flatten_df)

# Before writing final, check best partitioning column for shows_cleaned_df to handle skew. 
# column show_id has cardinality as 10 with reason as: Natural grouping episodes belong to a show. Joins with silver_shows benefit from co-located data.

# Write to silver table
episodes_cleaned_df.write.mode("overwrite").partitionBy("show_id").option("mergeSchema", "true").saveAsTable(episodes_silver_table_name)
# spark.sql(f"OPTIMIZE {episodes_silver_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    f"Silver table '{episodes_silver_table_name}' created",
    row_count=episodes_cleaned_df.count(),
    column_count=len(episodes_cleaned_df.columns)
)

DatabricksLogger.info(
    "Bronze to Silver transformation completed successfully",
    shows_silver_table=shows_silver_table_name,
    cast_silver_table=cast_silver_table_name,
    episodes_silver_table=episodes_silver_table_name
)
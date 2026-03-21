# Databricks notebook source
# Magic # 

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion Notebook

# COMMAND ----------

import sys

import requests
import json

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
bronze_schema_name = config["raw_adls_path"]
raw_adls_path = config["raw_adls_path"]
shows_url = config["shows_url"]
limit_shows = config.get("limit_shows", 10)

DatabricksLogger.info(
    "Starting raw data ingestion",
    bundle_root_path=bundle_root_path,
    environment=env,
    catalog_name =catalog_name,
    bronze_schema_name=bronze_schema_name,
    raw_adls_path=raw_adls_path,
    shows_url=shows_url
)
# COMMAND ----------

shows_raw_path = raw_adls_path.rstrip("/") + "/raw/shows/shows.json"
episodes_raw_path = raw_adls_path.rstrip("/") + "/raw/episodes/episodes.json"
cast_raw_path = raw_adls_path.rstrip("/") + "/raw/cast/cast.json"

DatabricksLogger.info(
    "Defined raw data paths",
    shows_raw_path=shows_raw_path,
    episodes_raw_path=episodes_raw_path,
    cast_raw_path=cast_raw_path
)

# COMMAND ----------

# Fetch shows data
shows_response = requests.get(shows_url)
shows_data = shows_response.json()
dbutils.fs.put(shows_raw_path, json.dumps(shows_data), True)
DatabricksLogger.info(
    "Fetched shows data",
    shows_raw_path=shows_raw_path,
    num_shows=len(shows_data)
)

# COMMAND ----------

# Fetch episodes data & cast for each show
episodes_all = []
cast_all = []

for show in shows_data[:limit_shows]:  # limit to first `limit_shows` shows for demo
    show_id = show['id']
    
    # Fetch episodes
    episodes_url = f"{shows_url}/{show_id}/episodes"
    episodes_response = requests.get(episodes_url)
    episodes_data = episodes_response.json()
    for ep in episodes_data:
        ep['show_id'] = show_id  # add show_id to episode data
    episodes_all.extend(episodes_data)
    
    # Fetch cast
    cast_url = f"{shows_url}/{show_id}/cast"
    cast_response = requests.get(cast_url)
    cast_data = cast_response.json()
    for cast_member in cast_data:
        cast_member['show_id'] = show_id  # add show_id to cast data
    cast_all.extend(cast_data)

# Write episodes data to ADLS
dbutils.fs.put(episodes_raw_path, json.dumps(episodes_all), True)
DatabricksLogger.info(
    "Fetched episodes data",
    episodes_raw_path=episodes_raw_path,
    num_episodes=len(episodes_all)
)

# Write cast data to ADLS
dbutils.fs.put(cast_raw_path, json.dumps(cast_all), True)
DatabricksLogger.info(
    "Fetched cast data",
    cast_raw_path=cast_raw_path,
    num_cast=len(cast_all)
)

DatabricksLogger.info("Raw data created successfully.")
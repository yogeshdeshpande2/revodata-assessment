# Databricks notebook source
# Magic # 


# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion Notebook

# COMMAND ----------

from nbformat import write
import requests
import json
from pyspark.sql.functions import col, explode_outer
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys
from pathlib import Path

# COMMAND ----------


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
from pyspark.sql.functions import col, broadcast
from utils.databricks_logger import DatabricksLogger

spark = Utilities.get_spark()

# COMMAND ----------

# Load config based on environment
env = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "t"
config = load_config(f"{bundle_root_path}/configs/env_config.yaml", env=env)
catalog_name = config["catalog_name"]
silver_schema_name = config["silver_schema_name"]
gold_schema_name = config["gold_schema_name"]

bronze_adls_path = config["bronze_adls_path"]
silver_adls_path = config["silver_adls_path"]
gold_adls_path = config["gold_adls_path"]

silver_shows_table = config["silver_shows_table"]
silver_episodes_table = config["silver_episodes_table"]
silver_cast_table = config["silver_cast_table"]

gold_fact_table = config["gold_fact_table"]
gold_episodes_per_season = config["gold_episodes_per_season"]
gold_avg_runtime_per_show = config["gold_avg_runtime_per_show"]
gold_top_cast = config["gold_top_cast"]
gold_most_common_genres = config["gold_most_common_genres"]

skew_handle_salt_count = config.get("skew_handle_salt_count", 10)  # Default to 10 if not specified

DatabricksLogger.info(
    "Configuration loaded successfully",
    bundle_root_path=bundle_root_path,
    environment=env,
    catalog_name=catalog_name,
    silver_schema_name=silver_schema_name,
    gold_schema_name=gold_schema_name,
    silver_shows_table=silver_shows_table,
    silver_episodes_table=silver_episodes_table,
    silver_cast_table=silver_cast_table,
    gold_fact_table=gold_fact_table,
    gold_episodes_per_season=gold_episodes_per_season,
    gold_avg_runtime_per_show=gold_avg_runtime_per_show,
    gold_top_cast=gold_top_cast,
    gold_most_common_genres=gold_most_common_genres,
    skew_handle_salt_count=skew_handle_salt_count
)

# COMMAND ----------

shows_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_shows_table}"
cast_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_cast_table}"
episodes_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_episodes_table}"

gold_fact_table_name = f"{catalog_name}.{gold_schema_name}.{gold_fact_table}"
gold_episodes_per_season_table_name = f"{catalog_name}.{gold_schema_name}.{gold_episodes_per_season}"
gold_avg_runtime_per_show_table_name = f"{catalog_name}.{gold_schema_name}.{gold_avg_runtime_per_show}"
gold_top_cast_table_name = f"{catalog_name}.{gold_schema_name}.{gold_top_cast}"
gold_most_common_genres_table_name = f"{catalog_name}.{gold_schema_name}.{gold_most_common_genres}"

DatabricksLogger.info(
    "Constructed gold fact table name",
    shows_silver_table_name=shows_silver_table_name,
    cast_silver_table_name=cast_silver_table_name,
    episodes_silver_table_name=episodes_silver_table_name,
    gold_fact_table_name=gold_fact_table_name,
    gold_episodes_per_season_table_name=gold_episodes_per_season_table_name,
    gold_avg_runtime_per_show_table_name=gold_avg_runtime_per_show_table_name,
    gold_top_cast_table_name=gold_top_cast_table_name,
    gold_most_common_genres_table_name=gold_most_common_genres_table_name,
    skew_handle_salt_count=skew_handle_salt_count
)

# COMMAND ----------

# Read silver tables
df_shows = spark.table(shows_silver_table_name)
df_episodes = spark.table(episodes_silver_table_name)
df_cast = spark.table(cast_silver_table_name)

# Broadcast the smaller tables (shows: 240 rows, cast: 170 rows)
df_gold_fact = (
    df_episodes.alias("e")
    .join(broadcast(df_shows).alias("s"), col("e.show_id") == col("s.id"), "inner")
    .join(broadcast(df_cast).alias("c"), col("e.show_id") == col("c.show_id"), "inner")
    .select(
        col("s.id").alias("show_id"),
        col("s.name").alias("show_name"),        
        col("s.language"),
        col("s.genre"),
        col("e.season"),
        col("e.name").alias("episode_name"),
        col("e.airdate"),
        col("e.runtime"),
        col("c.person_name").alias("cast_name"),
        col("c.character_name")
    )
)

# Before writing final, check best partitioning column for df_gold_fact to handle skew. 
# column genre  has cardinality as 12 with reason as: Natural analytical filter ("show me all Drama episodes"). Good cardinality, even distribution.

df_gold_fact.write.mode("overwrite").partitionBy("genre").saveAsTable(gold_fact_table_name)
spark.sql(f"OPTIMIZE {gold_fact_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_fact_table=gold_fact_table_name,
    row_count=df_gold_fact.count()
)


# COMMAND ----------

# 1. Episodes per season per show
episodes_per_season = (
    df_episodes
    .groupBy("show_id", "season")
    .agg(F.count("id").alias("episodes_count"))
)

episodes_per_season.write.mode("overwrite").saveAsTable(gold_episodes_per_season_table_name)
spark.sql(f"OPTIMIZE {gold_episodes_per_season_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_summary_table=gold_episodes_per_season_table_name,
    row_count=episodes_per_season.count()
)

# COMMAND ----------

# 2. Average runtime per show
avg_runtime_per_show = (
    df_episodes
    .groupBy("show_id")
    .agg(F.avg("runtime").alias("avg_runtime"))
)

avg_runtime_per_show.write.mode("overwrite").saveAsTable(gold_avg_runtime_per_show_table_name)
spark.sql(f"OPTIMIZE {gold_avg_runtime_per_show_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_summary_table=gold_avg_runtime_per_show_table_name,
    row_count=avg_runtime_per_show.count()
)

# COMMAND ----------

# 3. Top cast members per show (e.g., top 3 by appearances)

cast_appearances = (
    df_cast
    .groupBy("show_id", "person_name")
    .agg(F.count("*").alias("appearances"))
)


cast_window = Window.partitionBy("show_id").orderBy(F.desc("appearances"))
top_cast = (
    cast_appearances
    .withColumn("rank", F.row_number().over(cast_window))
    .filter(F.col("rank") <= 3)
    .select("show_id", "person_name", "appearances", "rank")
)

top_cast.write.mode("overwrite").saveAsTable(gold_top_cast_table_name)
spark.sql(f"OPTIMIZE {gold_top_cast_table_name} ZORDER BY (show_id)")

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_summary_table=gold_top_cast_table_name,
    row_count=top_cast.count()
)

# COMMAND ----------

# 4. Most common genres across all shows
most_common_genres = (
    df_shows
    .groupBy("genre")
    .agg(F.count("id").alias("genre_count"))
    .orderBy(F.desc("genre_count"))
)

most_common_genres.write.mode("overwrite").partitionBy("genre").saveAsTable(gold_most_common_genres_table_name)
# spark.sql(f"OPTIMIZE {gold_most_common_genres_table_name} ZORDER BY (genre)")

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_summary_table=gold_most_common_genres_table_name,
    row_count=most_common_genres.count()
)
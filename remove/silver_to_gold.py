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

silver_airbnb_table = config["silver_airbnb_table"]
silver_rentals_table = config["silver_rentals_table"]

gold_top_zipcodes_airbnb = config["gold_top_zipcodes_airbnb"]
gold_top_zipcodes_rentals = config["gold_top_zipcodes_rentals"]
gold_investment_comparison = config["gold_investment_comparison"]


DatabricksLogger.info(
    "Configuration loaded successfully",
    bundle_root_path=bundle_root_path,
    environment=env,
    catalog_name=catalog_name,
    silver_schema_name=silver_schema_name,
    gold_schema_name=gold_schema_name,
    silver_airbnb_table=silver_airbnb_table,
    silver_rentals_table=silver_rentals_table,
    gold_top_zipcodes_airbnb=gold_top_zipcodes_airbnb,
    gold_top_zipcodes_rentals=gold_top_zipcodes_rentals,
    gold_investment_comparison=gold_investment_comparison
)

# COMMAND ----------

airbnb_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_airbnb_table}"
rentals_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_rentals_table}"

gold_top_zipcodes_airbnb_table_name = f"{catalog_name}.{gold_schema_name}.{gold_top_zipcodes_airbnb}"
gold_top_zipcodes_rentals_table_name = f"{catalog_name}.{gold_schema_name}.{gold_top_zipcodes_rentals}"
gold_investment_comparison_table_name = f"{catalog_name}.{gold_schema_name}.{gold_investment_comparison}"

DatabricksLogger.info(
    "Constructed gold fact table name",
    airbnb_silver_table_name=airbnb_silver_table_name,
    rentals_silver_table_name=rentals_silver_table_name,
    gold_top_zipcodes_airbnb_table_name=gold_top_zipcodes_airbnb_table_name,
    gold_top_zipcodes_rentals_table_name=gold_top_zipcodes_rentals_table_name,
    gold_investment_comparison_table_name=gold_investment_comparison_table_name
)

# COMMAND ----------

occupancy_rate = config["occupancy_rate"]
days_per_year = config["days_per_year"]
months_per_year = config["months_per_year"]

DatabricksLogger.info(
    "Configuration loaded successfully",
    occupancy_rate=occupancy_rate,
    days_per_year=days_per_year,
    months_per_year=months_per_year
)


# COMMAND ----------

# Read silver table
df_silver_airbnb = spark.table(airbnb_silver_table_name)

df_gold_airbnb = (
    df_silver_airbnb
    .groupBy("postalcode4")
    .agg(
        psf.round(psf.avg("price"), 2).alias("avg_nightly_price"),
        psf.round(psf.avg("review_scores_value"), 1).alias("avg_review_score"),
        psf.round(psf.avg("bedrooms"), 1).alias("avg_bedrooms"),
        psf.count("*").alias("airbnb_listing_count")
    )
    .withColumn("est_annual_airbnb_revenue", psf.round(col("avg_nightly_price") * days_per_year * occupancy_rate, 2))
)

window_spec = Window.orderBy(df_gold_airbnb.est_annual_airbnb_revenue.desc())
df_gold_airbnb = df_gold_airbnb.withColumn("airbnb_revenue_rank", psf.rank().over(window_spec))

df_gold_airbnb = df_gold_airbnb.withColumn("occupancy_rate", psf.lit(occupancy_rate))

df_gold_airbnb.write.mode("append").saveAsTable(gold_top_zipcodes_airbnb_table_name)

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_top_zipcodes_airbnb_table=gold_top_zipcodes_airbnb_table_name,
    row_count=df_gold_airbnb.count()
)

# COMMAND ----------

# Read silver table
df_silver_rentals = spark.table(rentals_silver_table_name)

df_gold_rentals = (
    df_silver_rentals
    .groupBy("postalcode4")
    .agg(
        psf.round(psf.avg("monthly_rent"), 2).alias("avg_monthly_rent"),
        psf.round(psf.avg("area_sqm"), 1).alias("avg_area_sqm"),        
        psf.count("*").alias("rental_listing_count")
    )
    .withColumn("est_annual_rental_revenue", psf.round(col("avg_monthly_rent") * months_per_year, 2))
)

window_spec = Window.orderBy(df_gold_rentals.est_annual_rental_revenue.desc())
df_gold_rentals = df_gold_rentals.withColumn("rental_revenue_rank", psf.rank().over(window_spec))

df_gold_rentals.write.mode("append").saveAsTable(gold_top_zipcodes_rentals_table_name)

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_top_zipcodes_rentals_table=gold_top_zipcodes_rentals_table_name,
    row_count=df_gold_rentals.count()
)

# COMMAND ----------
rank_window = Window.orderBy(psf.col("revenue_airbnb_minus_rentals").desc())

investment_comparison = (
    df_gold_airbnb
    .join(df_gold_rentals, on="postalcode4", how="inner")
    .withColumn("revenue_airbnb_minus_rentals", psf.round(psf.col("est_annual_airbnb_revenue") - psf.col("est_annual_rental_revenue"), 2))
    .withColumn("revenue_ratio", psf.round(psf.col("est_annual_airbnb_revenue") / psf.col("est_annual_rental_revenue"), 2))
    .withColumn("total_listings", psf.col("airbnb_listing_count") + psf.col("rental_listing_count"))
    .withColumn("recommended_strategy",
        psf.when(psf.col("revenue_airbnb_minus_rentals") > 0, "Airbnb (Short-term)")
        .otherwise("Kamernet (Long-term)"))
    .withColumn("investment_rank", psf.rank().over(
        rank_window
        ))
    .orderBy("investment_rank")
)

investment_comparison.write.mode("overwrite").saveAsTable(gold_investment_comparison_table_name)

DatabricksLogger.info(
    "Silver to Gold transformation completed successfully",
    gold_investment_comparison_table=gold_investment_comparison_table_name,
    row_count=investment_comparison.count()
)
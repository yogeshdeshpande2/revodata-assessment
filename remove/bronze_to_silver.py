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

from src.bronze_to_silver import BronzeToSilver

config_path = f"{bundle_root_path}/configs/env_config.yaml"
transformer = BronzeToSilver(bundle_root_path, dbutils, config_path)
transformer.run()

# if __name__ == "__main__":
#     config_path = f"{bundle_root_path}/configs/env_config.yaml"
#     transformer = BronzeToSilver(bundle_root_path, dbutils, config_path)
#     transformer.run()

# from utils import Utilities
# from utils.config_loader import load_config
# from utils.databricks_logger import DatabricksLogger


# # Get Spark session
# spark = Utilities.get_spark()

# # COMMAND ----------

# # Load config based on environment
# env = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "t"
# config = load_config(f"{bundle_root_path}/configs/env_config.yaml", env=env)
# catalog_name = config["catalog_name"]
# bronze_schema_name = config["bronze_schema_name"]
# silver_schema_name = config["silver_schema_name"]

# bronze_adls_path = config["bronze_adls_path"]
# silver_adls_path = config["silver_adls_path"]

# bronze_airbnb_table = config["bronze_airbnb_table"]
# bronze_rentals_table = config["bronze_rentals_table"]
# bronze_postcode_table = config["bronze_postcode_table"]

# silver_airbnb_table = config["silver_airbnb_table"]
# silver_rentals_table = config["silver_rentals_table"]

# DatabricksLogger.info(
#     "Configuration loaded successfully",
#     bundle_root_path=bundle_root_path,
#     environment=env,
#     catalog_name=catalog_name,
#     bronze_schema_name=bronze_schema_name,
#     silver_schema_name=silver_schema_name,
#     bronze_adls_path=bronze_adls_path,
#     silver_adls_path=silver_adls_path,
#     bronze_airbnb_table=bronze_airbnb_table,
#     bronze_rentals_table=bronze_rentals_table,
#     bronze_postcode_table=bronze_postcode_table,
#     silver_airbnb_table=silver_airbnb_table,
#     silver_rentals_table=silver_rentals_table   
# )

# # COMMAND ----------

# bronze_airbnb_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_airbnb_table}"
# bronze_rentals_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_rentals_table}"
# bronze_postcode_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_postcode_table}"

# silver_airbnb_table_name = f"{catalog_name}.{silver_schema_name}.{silver_airbnb_table}"
# silver_rentals_table_name = f"{catalog_name}.{silver_schema_name}.{silver_rentals_table}"

# DatabricksLogger.info(
#     "Constructed bronze table names",
#     bronze_airbnb_table_name=bronze_airbnb_table_name,
#     bronze_rentals_table_name=bronze_rentals_table_name,
#     bronze_postcode_table_name=bronze_postcode_table_name
# )

# DatabricksLogger.info(
#     "Constructed silver table names",
#     silver_airbnb_table_name=silver_airbnb_table_name,
#     silver_rentals_table_name=silver_rentals_table_name
# )

# # COMMAND ----------

# # MAGIC %md
# # MAGIC # airbnb silver populate

# # COMMAND ----------

# # Read bronze table and flatten
# df_bronze_airbnb = spark.read.table(bronze_airbnb_table_name)
# df_bronze_postcode = spark.read.table(bronze_postcode_table_name)

# df_bronze_airbnb = df_bronze_airbnb.withColumn('zipcode_formatted', Utilities.format_zipcode('zipcode'))

# df_bronze_airbnb_coords = df_bronze_airbnb.filter(psf.col('zipcode_formatted').isNull()).select("latitude", "longitude") \
#     .dropna() \
#     .dropDuplicates()


# df_bronze_postcode_flat = df_bronze_postcode.withColumn("feature", psf.explode("features"))
# df_bronze_postcode_flat = df_bronze_postcode_flat.select(
# 	psf.col("feature.properties.pc4_code").alias("postcode"),
# 	psf.col("feature.geometry").alias("geometry")
# )

# # Convert struct → JSON string
# df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn("geometry_json", psf.to_json(psf.col("geometry")))

# # 🔥 FIX: remove quotes around coordinates (string → array)
# df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
#     "geometry_json_clean",
#     psf.regexp_replace(psf.col("geometry_json"), r'"\[', '[')
# )

# df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
#     "geometry_json_clean",
#     psf.regexp_replace(psf.col("geometry_json_clean"), r'\]"', ']')
# )

# df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
#     "geometry_json_clean",
#     psf.regexp_replace(psf.col("geometry_json_clean"), '"(-?\\d+\\.?\\d*)"', '$1')
# )

# # Create polygon
# df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
#     "polygon",
#     psf.expr("ST_GeomFromGeoJSON(geometry_json_clean)")
# )

# # Prepare Airbnb points with missing zipcodes
# df_bronze_airbnb_coords = df_bronze_airbnb.filter(psf.col("zipcode_formatted").isNull()) \
#     .dropna(subset=["latitude", "longitude"]) \
#     .dropDuplicates(["latitude", "longitude"]) \
#     .withColumn("point", psf.expr("ST_Point(longitude, latitude, 4326)"))

# # Spatial join: find which postcode polygon contains each point
# df_bronze_airbnb_coords.createOrReplaceTempView("points")
# df_bronze_postcode_flat.createOrReplaceTempView("postcodes")

# result = spark.sql("""
# SELECT p.*, pc.postcode
# FROM points p, postcodes pc
# WHERE ST_Contains(pc.polygon, p.point)
# """)

# # result.select("latitude", "longitude", "postcode").show(100)
# df_silver_airbnb = df_bronze_airbnb.join(result.select("latitude", "longitude", "postcode"), on=["latitude", "longitude"], how="left")
# df_silver_airbnb = df_silver_airbnb.withColumn("final_zipcode", psf.when(psf.col("zipcode_formatted").isNull(), psf.col("postcode")).otherwise(psf.col("zipcode_formatted")))


# df_silver_airbnb = (
#     df_silver_airbnb
#     .withColumn("postalcode4", psf.regexp_extract(psf.col("final_zipcode"), r"(\d{4})", 1))
#     .withColumn("price", psf.col("price").cast("double"))
#     .withColumn("review_scores_value", psf.col("review_scores_value").cast("double"))
#     .withColumn("bedrooms", psf.col("bedrooms").cast("double"))
#     .filter(psf.col("postalcode4") != "")
#     .filter(psf.col("price").isNotNull())
# )


# # Write to silver table
# df_silver_airbnb.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(silver_airbnb_table_name)

# DatabricksLogger.info(
#     f"Silver table '{silver_airbnb_table_name}' created",
#     row_count=df_silver_airbnb.count(),
#     column_count=len(df_silver_airbnb.columns)
# )

# # COMMAND ----------

# # MAGIC %md
# # MAGIC # rentals silver populate

# # COMMAND ----------

# df_bronze_rentals = spark.read.table(bronze_rentals_table_name)
# df_bronze_rentals = df_bronze_rentals.withColumn("zipcode_formatted", Utilities.format_zipcode("postalCode"))

# print("Sample of formatted zipcodes:")
# df_bronze_rentals.select('postalCode', 'zipcode_formatted').show(25)

# formatted_zipcode_null_count = df_bronze_rentals.filter(psf.col('zipcode_formatted').isNull()).count()
# DatabricksLogger.info(
#     "Zipcode formatting completed",
#     formatted_zipcode_null_count=formatted_zipcode_null_count
# )


# # List of array columns to explode
# # array_columns = ['_id', 'crawledAt', 'detailsCrawledAt', 'firstSeenAt', 'lastSeenAt']
# array_columns = [field.name for field in df_bronze_rentals.schema.fields if isinstance(field.dataType, ArrayType)]

# df_bronze_rentals_cleaned = df_bronze_rentals
# for col_name in array_columns:
#     if col_name in df_bronze_rentals_cleaned.columns:
#         df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.withColumn(col_name, psf.explode(psf.col(col_name)))

# # Drop any remaining array columns
# array_cols_to_drop = [f.name for f in df_bronze_rentals_cleaned.schema.fields if isinstance(f.dataType, ArrayType)]
# df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.drop(*array_cols_to_drop)

# # Clean string columns: trim and convert empty strings to null
# for field in df_bronze_rentals_cleaned.schema.fields:
#     if str(field.dataType) == "StringType":
#         df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.withColumn(
#             field.name,
#             psf.when(psf.trim(psf.col(field.name)) == "", None).otherwise(psf.trim(psf.col(field.name)))
#         )

# df_rentals_clean = (
#     df_bronze_rentals_cleaned
#     .withColumn("postalcode4", psf.regexp_extract(psf.col("postalCode"), r"(\d{4})", 1))
#     .withColumn("monthly_rent", psf.regexp_extract(psf.col("rent"), r"€\s*(\d+)", 1).cast("double"))
#     .withColumn("utilities_included", psf.col("rent").contains("Utilities incl."))
#     .withColumn("area_sqm", psf.regexp_extract(psf.col("areaSqm"), r"(\d+)", 1).cast("double"))
#     .filter(psf.col("postalcode4") != "")
#     .filter(psf.col("monthly_rent").isNotNull())
#     .filter(psf.col("monthly_rent") > 0)
# )

# df_rentals_clean.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(silver_rentals_table_name)

# DatabricksLogger.info(
#     f"Silver table '{silver_rentals_table_name}' created",
#     row_count=df_rentals_clean.count(),
#     column_count=len(df_rentals_clean.columns)
# )
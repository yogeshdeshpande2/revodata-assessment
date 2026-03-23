# Import PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from sedona.spark import SedonaContext

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ExploreData") \
    .config("spark.jars.packages", "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:geotools-24.0") \
    .getOrCreate()


# Read files from data/rentals
df_rentals = spark.read.format('json').load("data/rentals/rentals.json")
print("Top 5 rows from Rentals data:")
df_rentals.show(2)

rentals_null_zipcode_count = df_rentals.filter(psf.col('postalCode').isNull()).count()
print(f"Number of rows with null zipcode: {rentals_null_zipcode_count}")

# Validate and format 'zipcode' column
from pyspark.sql.functions import length, when, col, lit, regexp_replace

def format_zipcode(zipcode_col):
	# If length is 7, keep as is
	# If length is 6, insert space after 4th char (Dutch zipcodes)
	# If length is 4 or null, set to None
	return (
		when(col(zipcode_col).isNull(), None)
		.when(length(col(zipcode_col)) == 7, col(zipcode_col))
		.when(length(col(zipcode_col)) == 6, regexp_replace(col(zipcode_col), r"^(.{4})(.{2})$", r"$1 $2"))
		.when(length(col(zipcode_col)) == 4, col(zipcode_col))
		.otherwise(None)
	)

df_rentals = df_rentals.withColumn('zipcode_formatted', format_zipcode('postalCode'))

print("Sample of formatted zipcodes:")
df_rentals.select('postalCode', 'zipcode_formatted').show(25)

formatted_zipcode_null_count = df_rentals.filter(psf.col('zipcode_formatted').isNull()).count()
print(f"Number of rows with null formatted zipcode: {formatted_zipcode_null_count}")

df_rentals.printSchema()

from pyspark.sql.functions import explode, trim, when

# List of array columns to explode
array_columns = ['_id', 'crawledAt', 'detailsCrawledAt', 'firstSeenAt', 'lastSeenAt']

df_cleaned = df_rentals
for col_name in array_columns:
    if col_name in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn(col_name, explode(col(col_name)))

# Drop any remaining array columns
from pyspark.sql.types import ArrayType
array_cols_to_drop = [f.name for f in df_cleaned.schema.fields if isinstance(f.dataType, ArrayType)]
df_cleaned = df_cleaned.drop(*array_cols_to_drop)

# Clean string columns: trim and convert empty strings to null
for field in df_cleaned.schema.fields:
    if str(field.dataType) == "StringType":
        df_cleaned = df_cleaned.withColumn(
            field.name,
            when(trim(col(field.name)) == "", None).otherwise(trim(col(field.name)))
        )

df_cleaned.show(5)
df_cleaned.printSchema()

# # # Read files from data/geo/amsterdam_areas/amsterdam_areas.geojson
# # df_rentals = spark.read.format('json').load("data/rentals/rentals.json")
# # print("Top 5 rows from Rentals data:")
# # # df_rentals.show(5)

# # coords_df = df_airbnb.select("latitude", "longitude") \
# #     .dropna() \
# #     .dropDuplicates()

# print(f"Number of df_airbnb rows: {df_airbnb.count()}")
# print(f"Number of unique coordinates: {coords_df.count()}")



# # After SparkSession is created
# sc = SedonaContext.create(spark)


# # Load postcode polygons GeoJSON
# from pyspark.sql.functions import to_json
# postcode_df = spark.read.option("multiline", "true").json("data/geo/post_codes/post_codes.geojson")
# postcode_flat = postcode_df.withColumn("feature", psf.explode("features"))
# postcode_flat = postcode_flat.select(
# 	col("feature.properties.pc4_code").alias("postcode"),
# 	col("feature.geometry").alias("geometry")
# )
# # Convert geometry struct to JSON string before passing to ST_GeomFromGeoJSON
# # postcode_flat = postcode_flat.withColumn("geometry_json", to_json(col("geometry")))
# # postcode_flat = postcode_flat.withColumn("polygon", expr("ST_GeomFromGeoJSON(geometry_json)"))

# from pyspark.sql.functions import regexp_replace

# # Convert struct → JSON string
# postcode_flat = postcode_flat.withColumn("geometry_json", to_json(col("geometry")))

# # 🔥 FIX: remove quotes around coordinates (string → array)
# postcode_flat = postcode_flat.withColumn(
#     "geometry_json_clean",
#     regexp_replace(col("geometry_json"), r'"\[', '[')
# )

# postcode_flat = postcode_flat.withColumn(
#     "geometry_json_clean",
#     regexp_replace(col("geometry_json_clean"), r'\]"', ']')
# )

# # Create polygon
# postcode_flat = postcode_flat.withColumn(
#     "polygon",
#     expr("ST_GeomFromGeoJSON(geometry_json_clean)")
# )

# # Prepare Airbnb points with missing zipcodes
# coords_df = df_airbnb.filter(col("zipcode_formatted").isNull()) \
#     .dropna(subset=["latitude", "longitude"]) \
#     .dropDuplicates(["latitude", "longitude"]) \
#     .withColumn("point", expr("ST_Point(longitude, latitude)"))

# # Spatial join: find which postcode polygon contains each point
# coords_df.createOrReplaceTempView("points")
# postcode_flat.createOrReplaceTempView("postcodes")

# result = spark.sql("""
# SELECT p.*, pc.postcode
# FROM points p, postcodes pc
# WHERE ST_Contains(pc.polygon, p.point)
# """)

# result.select("latitude", "longitude", "postcode").show(100)
# # result.printSchema()

# result2 = result.select("latitude", "longitude", "postcode")
# # df_airbnb2 = df_airbnb.filter(psf.col('zipcode_formatted').isNull()).select("latitude", "longitude").dropna().dropDuplicates()



# final_df = df_airbnb.join(result2, on=["latitude", "longitude"], how="left")

# final_df = final_df.withColumn("final_zipcode", when(col("zipcode_formatted").isNull(), col("postcode")).otherwise(col("zipcode_formatted")))
# final_df.show(100)

# # final_df.filter(psf.col('zipcode_formatted').isNull()).show(100)


# # df_airbnb_v2 = df_airbnb.filter(psf.col('zipcode_formatted').isNull()).join(result.select("latitude", "longitude", "postcode"), on=["latitude", "longitude"], how="left") \

# # df_airbnb_v2.show(2)

# # import requests

# # def reverse_geocode(lat, lon):
# #     """
# #     Returns zipcode (postcode) for given latitude & longitude
# #     using OpenStreetMap Nominatim API
# #     """
# #     try:
# #         url = "https://nominatim.openstreetmap.org/reverse"
# #         params = {
# #             "format": "json",
# #             "lat": lat,
# #             "lon": lon,
# #             "addressdetails": 1
# #         }
# #         headers = {
# #             "User-Agent": "pyspark-reverse-geocode"
# #         }

# #         response = requests.get(url, params=params, headers=headers, timeout=10)

# #         if response.status_code == 200:
# #             data = response.json()
# #             return data.get("address", {}).get("postcode", None)
# #         else:
# #             return None

# #     except Exception:
# #         return None
    
# # zipcode = reverse_geocode(52.36575451, 4.941419235)  # Amsterdam coords
# # print(zipcode)

# # # Read files from data/rentals
# # df_rentals = spark.read.format('json').load("data/rentals/rentals.json")
# # print("Top 5 rows from Rentals data:")
# # # df_rentals.show(5)

# # from pyspark.sql.functions import explode, col

# # # List of array columns to flatten
# # array_columns = ['_id', 'crawledAt', 'detailsCrawledAt', 'firstSeenAt', 'lastSeenAt']

# # # Explode each array column (if present)
# # df_cleaned_rentals = df_rentals
# # for arr_col in array_columns:
# #     if arr_col in df_cleaned_rentals.columns:
# #         df_cleaned_rentals = df_cleaned_rentals.withColumn(arr_col, explode(col(arr_col)))

# # # Optionally, drop any remaining array columns (if you want to remove, not flatten)
# # from pyspark.sql.types import ArrayType

# # array_cols_to_drop = [f.name for f in df_cleaned_rentals.schema.fields if isinstance(f.dataType, ArrayType)]
# # df_cleaned_rentals = df_cleaned_rentals.drop(*array_cols_to_drop)

# # # Show the cleaned DataFrame
# # print("Top 5 rows from Rentals data:")
# # # df_cleaned_rentals.show(5)
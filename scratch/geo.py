from pyspark.sql import SparkSession
from pyspark.sql import functions as psf

# Create Spark session
spark = SparkSession.builder \
    .appName("Read GeoJSON File") \
    .getOrCreate()

file_path = "data/geo/post_codes/post_codes.geojson"

# # Path to your file
# file_path = "data/geo/amsterdam_areas/amsterdam_areas.geojson"

# Read GeoJSON (as JSON)
df = spark.read.option("multiline", "true").json(file_path)

# Show schema
df.printSchema()

# Show raw data
df.show(1)

from bundle.utils.utils import Utilities

# After exploding features:
df_flat = df.withColumn("feature", psf.explode("features"))
# df_flat = df_flat.select(*Utilities.flatten_df(df_flat.schema))

df_flat.select('feature').show(1, False)

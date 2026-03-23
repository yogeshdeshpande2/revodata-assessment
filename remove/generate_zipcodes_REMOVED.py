from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from sedona.spark import SedonaContext
from bundle.utils.utils import Utilities
from pyspark.sql.functions import to_json
from pyspark.sql.functions import regexp_replace

# Initialize Spark session
spark = Utilities.get_spark()

# After SparkSession is created
sc = SedonaContext.create(spark)

# Load postcode polygons GeoJSON
postcode_df = spark.read.option("multiline", "true").json("data/geo/post_codes/post_codes.geojson")
postcode_flat = postcode_df.withColumn("feature", psf.explode("features"))
postcode_flat = postcode_flat.select(
	col("feature.properties.pc4_code").alias("postcode"),
	col("feature.geometry").alias("geometry")
)
# Convert geometry struct to JSON string before passing to ST_GeomFromGeoJSON
# postcode_flat = postcode_flat.withColumn("geometry_json", to_json(col("geometry")))
# postcode_flat = postcode_flat.withColumn("polygon", expr("ST_GeomFromGeoJSON(geometry_json)"))

# Convert struct → JSON string
postcode_flat = postcode_flat.withColumn("geometry_json", to_json(col("geometry")))

# 🔥 FIX: remove quotes around coordinates (string → array)
postcode_flat = postcode_flat.withColumn(
    "geometry_json_clean",
    regexp_replace(col("geometry_json"), r'"\[', '[')
)

postcode_flat = postcode_flat.withColumn(
    "geometry_json_clean",
    regexp_replace(col("geometry_json_clean"), r'\]"', ']')
)

# Create polygon
postcode_flat = postcode_flat.withColumn(
    "polygon",
    expr("ST_GeomFromGeoJSON(geometry_json_clean)")
)

# Spatial join: find which postcode polygon contains each point
coords_df.createOrReplaceTempView("points")
postcode_flat.createOrReplaceTempView("postcodes")

result = spark.sql("""
SELECT p.*, pc.postcode
FROM points p, postcodes pc
WHERE ST_Contains(pc.polygon, p.point)
""")

result.select("latitude", "longitude", "postcode").show(100)
result.printSchema()
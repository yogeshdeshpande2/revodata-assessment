from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

# Import PySpark modules
from pyspark.sql import functions as psf
from pyspark.sql.functions import expr
# from sedona.spark import SedonaContext
from pyspark.sql.functions import length, when, col, lit, regexp_replace

class Utilities:

    def __init__(self):
        pass

    @staticmethod
    def flatten_df(schema, prefix=""):
        """
        Recursively flattens a StructType schema for use in DataFrame select.
        Returns a list of column expressions with aliases for nested fields.
        """
        flat_cols = []
        for field in schema.fields:
            col_name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                flat_cols.extend(Utilities.flatten_df(field.dataType, col_name))
            else:
                alias = col_name.replace(".", "_")
                flat_cols.append(col(col_name).alias(alias))
        return flat_cols

    @staticmethod
    def handle_null_stringcols(df):
        string_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() == "string"]
        for col_name in string_cols:
            df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))
        return df

    @staticmethod
    def get_spark():        
        try:
            spark
        except NameError:            
            # spark = SparkSession.builder.getOrCreate()    
            spark = SparkSession.builder \
                .appName("ExploreData") \
                .config("spark.jars.packages", "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:geotools-24.0") \
                .getOrCreate()            
        return spark
    
    @staticmethod
    def get_dbutils():
        try:
            dbutils
        except NameError:
            try:
                from pyspark.dbutils import DBUtils  # type: ignore
            except ImportError:
                DBUtils = None
            if DBUtils is not None:
                dbutils = DBUtils(Utilities.get_spark())  # type: ignore
                return dbutils
            else:
                return None
        return dbutils
    
    @staticmethod
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
    
    @staticmethod
    def get_geo_postcodes(spark, postcode_df):        
        
        sc = SedonaContext.create(spark)

        from pyspark.sql.functions import to_json
        postcode_flat = postcode_df.withColumn("feature", psf.explode("features"))
        postcode_flat = postcode_flat.select(
            col("feature.properties.pc4_code").alias("postcode"),
            col("feature.geometry").alias("geometry")
        )
        # Convert geometry struct to JSON string before passing to ST_GeomFromGeoJSON
        # postcode_flat = postcode_flat.withColumn("geometry_json", to_json(col("geometry")))
        # postcode_flat = postcode_flat.withColumn("polygon", expr("ST_GeomFromGeoJSON(geometry_json)"))

        from pyspark.sql.functions import regexp_replace

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
        return postcode_flat 
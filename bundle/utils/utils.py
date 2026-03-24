from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
import yaml


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
                flat_cols.append(psf.col(col_name).alias(alias))
        return flat_cols

    @staticmethod
    def handle_null_stringcols(df):
        string_cols = [
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "string"
        ]
        for col_name in string_cols:
            df = df.withColumn(
                col_name,
                psf.when(psf.col(col_name).isNull(), "Unknown").otherwise(
                    psf.col(col_name)
                ),
            )
        return df

    @staticmethod
    def get_spark():
        try:
            spark
        except NameError:
            # spark = SparkSession.builder.getOrCreate()
            spark = (
                SparkSession.builder.appName("ExploreData")
                .config(
                    "spark.jars.packages",
                    "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:geotools-24.0",
                )
                .getOrCreate()
            )
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
            psf.when(psf.col(zipcode_col).isNull(), None)
            .when(psf.length(psf.col(zipcode_col)) == 7, psf.col(zipcode_col))
            .when(
                psf.length(psf.col(zipcode_col)) == 6,
                psf.regexp_replace(psf.col(zipcode_col), r"^(.{4})(.{2})$", r"$1 $2"),
            )
            .when(psf.length(psf.col(zipcode_col)) == 4, psf.col(zipcode_col))
            .otherwise(None)
        )

    @staticmethod
    def load_config(config_path, env=None):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        env = env or config.get("env", "t")
        return config["variables"][env]

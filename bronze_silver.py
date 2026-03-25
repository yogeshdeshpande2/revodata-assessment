from pyspark.sql import functions as psf
from pyspark.sql.types import ArrayType
from bundle.utils import Utilities
from bundle.utils.databricks_logger import DatabricksLogger


class BronzeToSilver:
    """
    Class to handle the transformation from Bronze to Silver layer in the data pipeline.
    """

    def __init__(self, bundle_root_path, dbutils, config_path, env="t"):
        """
        Initializes the BronzeToSilver transformer with necessary configurations and utilities.
        """
        self.bundle_root_path = bundle_root_path
        self.dbutils = dbutils
        self.env = env
        self.Utilities = Utilities
        self.DatabricksLogger = DatabricksLogger
        self.spark = Utilities.get_spark()
        self.config = Utilities.load_config(config_path, env=env)
        self._extract_config_vars()

    def _extract_config_vars(self):
        """
        Extracts necessary configuration variables from the loaded config and constructs full table names.
        """

        self.catalog_name = self.config["catalog_name"]
        self.bronze_schema_name = self.config["bronze_schema_name"]
        self.silver_schema_name = self.config["silver_schema_name"]
        self.bronze_airbnb_table = self.config["bronze_airbnb_table"]
        self.bronze_rentals_table = self.config["bronze_rentals_table"]
        self.bronze_postcode_table = self.config["bronze_postcode_table"]
        self.silver_airbnb_table = self.config["silver_airbnb_table"]
        self.silver_rentals_table = self.config["silver_rentals_table"]
        self.bronze_airbnb_table_name = (
            f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_airbnb_table}"
        )
        self.bronze_rentals_table_name = (
            f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_rentals_table}"
        )
        self.bronze_postcode_table_name = f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_postcode_table}"
        self.silver_airbnb_table_name = (
            f"{self.catalog_name}.{self.silver_schema_name}.{self.silver_airbnb_table}"
        )
        self.silver_rentals_table_name = (
            f"{self.catalog_name}.{self.silver_schema_name}.{self.silver_rentals_table}"
        )

        self.DatabricksLogger.info(
            "Configuration variables extracted",
            catalog_name=self.catalog_name,
            bronze_schema_name=self.bronze_schema_name,
            silver_schema_name=self.silver_schema_name,
            bronze_airbnb_table=self.bronze_airbnb_table,
            bronze_rentals_table=self.bronze_rentals_table,
            bronze_postcode_table=self.bronze_postcode_table,
            silver_airbnb_table=self.silver_airbnb_table,
            silver_rentals_table=self.silver_rentals_table,
            bronze_airbnb_table_name=self.bronze_airbnb_table_name,
            bronze_rentals_table_name=self.bronze_rentals_table_name,
            bronze_postcode_table_name=self.bronze_postcode_table_name,
            silver_airbnb_table_name=self.silver_airbnb_table_name,
            silver_rentals_table_name=self.silver_rentals_table_name,
        )

    def run(self):
        """
        Executes the transformation logic to convert Bronze tables to Silver tables.
        """
        self.DatabricksLogger.info("Starting Bronze to Silver transformation")
        # self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.bronze_schema_name}")
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.silver_schema_name}")

        self._process_airbnb()
        self._process_rentals()
        self.DatabricksLogger.info("Bronze to Silver transformation completed")

    def _process_airbnb(self):
        """
        Processes the Airbnb data from the Bronze layer, performs necessary transformations and joins with postcode data, and writes the cleaned data to the Silver layer.
        """

        df_bronze_airbnb = self.spark.read.table(self.bronze_airbnb_table_name)
        df_bronze_postcode = self.spark.read.table(self.bronze_postcode_table_name)
        df_bronze_airbnb = df_bronze_airbnb.withColumn(
            "zipcode_formatted", self.Utilities.format_zipcode("zipcode")
        )

        df_bronze_postcode_flat = df_bronze_postcode.withColumn(
            "feature", psf.explode("features")
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.select(
            psf.col("feature.properties.pc4_code").alias("postcode"),
            psf.col("feature.geometry").alias("geometry"),
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
            "geometry_json", psf.to_json(psf.col("geometry"))
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
            "geometry_json_clean",
            psf.regexp_replace(psf.col("geometry_json"), r'"\[', "["),
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
            "geometry_json_clean",
            psf.regexp_replace(psf.col("geometry_json_clean"), r'\]"', "]"),
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
            "geometry_json_clean",
            psf.regexp_replace(
                psf.col("geometry_json_clean"), '"(-?\\d+\\.?\\d*)"', "$1"
            ),
        )
        df_bronze_postcode_flat = df_bronze_postcode_flat.withColumn(
            "polygon", psf.expr("ST_GeomFromGeoJSON(geometry_json_clean)")
        )

        df_bronze_airbnb_coords = (
            df_bronze_airbnb.filter(psf.col("zipcode_formatted").isNull())
            .dropna(subset=["latitude", "longitude"])
            .dropDuplicates(["latitude", "longitude"])
            .withColumn("point", psf.expr("ST_Point(longitude, latitude, 4326)"))
        )

        df_bronze_airbnb_coords.createOrReplaceTempView("points")
        df_bronze_postcode_flat.createOrReplaceTempView("postcodes")

        result = self.spark.sql("""
        SELECT p.*, pc.postcode
        FROM points p, postcodes pc
        WHERE ST_Contains(pc.polygon, p.point)
        """)

        df_silver_airbnb = df_bronze_airbnb.join(
            result.select("latitude", "longitude", "postcode"),
            on=["latitude", "longitude"],
            how="left",
        )
        df_silver_airbnb = df_silver_airbnb.withColumn(
            "final_zipcode",
            psf.when(
                psf.col("zipcode_formatted").isNull(), psf.col("postcode")
            ).otherwise(psf.col("zipcode_formatted")),
        )

        df_silver_airbnb = (
            df_silver_airbnb.withColumn(
                "postalcode4",
                psf.regexp_extract(psf.col("final_zipcode"), r"(\d{4})", 1),
            )
            .withColumn("price", psf.col("price").cast("double"))
            .withColumn(
                "review_scores_value", psf.col("review_scores_value").cast("double")
            )
            .withColumn("bedrooms", psf.col("bedrooms").cast("double"))
            .filter(psf.col("postalcode4") != "")
            .filter(psf.col("price").isNotNull())
        )

        df_silver_airbnb = df_silver_airbnb.drop_duplicates()

        df_silver_airbnb.write.mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(self.silver_airbnb_table_name)

        self.DatabricksLogger.info(
            f"Silver table '{self.silver_airbnb_table_name}' created",
            row_count=df_silver_airbnb.count(),
            column_count=len(df_silver_airbnb.columns),
        )

    def _process_rentals(self):
        """
        Processes the Rentals data from the Bronze layer, performs necessary transformations and cleaning, and writes the cleaned data to the Silver layer.
        """
        df_bronze_rentals = self.spark.read.table(self.bronze_rentals_table_name)
        df_bronze_rentals = df_bronze_rentals.withColumn(
            "zipcode_formatted", self.Utilities.format_zipcode("postalCode")
        )

        array_columns = [
            field.name
            for field in df_bronze_rentals.schema.fields
            if isinstance(field.dataType, ArrayType)
        ]
        df_bronze_rentals_cleaned = df_bronze_rentals
        for col_name in array_columns:
            if col_name in df_bronze_rentals_cleaned.columns:
                df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.withColumn(
                    col_name, psf.explode(psf.col(col_name))
                )

        array_cols_to_drop = [
            f.name
            for f in df_bronze_rentals_cleaned.schema.fields
            if isinstance(f.dataType, ArrayType)
        ]
        df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.drop(*array_cols_to_drop)

        for field in df_bronze_rentals_cleaned.schema.fields:
            if str(field.dataType) == "StringType":
                df_bronze_rentals_cleaned = df_bronze_rentals_cleaned.withColumn(
                    field.name,
                    psf.when(psf.trim(psf.col(field.name)) == "", None).otherwise(
                        psf.trim(psf.col(field.name))
                    ),
                )

        df_rentals_clean = (
            df_bronze_rentals_cleaned.withColumn(
                "postalcode4", psf.regexp_extract(psf.col("postalCode"), r"(\d{4})", 1)
            )
            .withColumn(
                "monthly_rent",
                psf.regexp_extract(psf.col("rent"), r"€\s*(\d+)", 1).cast("double"),
            )
            .withColumn(
                "utilities_included", psf.col("rent").contains("Utilities incl.")
            )
            .withColumn(
                "area_sqm",
                psf.regexp_extract(psf.col("areaSqm"), r"(\d+)", 1).cast("double"),
            )
            .filter(psf.col("postalcode4") != "")
            .filter(psf.col("monthly_rent").isNotNull())
            .filter(psf.col("monthly_rent") > 0)
        )

        df_rentals_clean = df_rentals_clean.drop_duplicates()

        df_rentals_clean.write.mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(self.silver_rentals_table_name)

        self.DatabricksLogger.info(
            f"Silver table '{self.silver_rentals_table_name}' created",
            row_count=df_rentals_clean.count(),
            column_count=len(df_rentals_clean.columns),
        )

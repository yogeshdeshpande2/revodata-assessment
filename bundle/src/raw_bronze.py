from bundle.utils import Utilities
from bundle.utils.databricks_logger import DatabricksLogger


class RawToBronze:
    """
    Class to handle the transformation from Raw to Bronze layer in the data pipeline.
    """

    def __init__(self, bundle_root_path, dbutils, config_path, env="t"):
        """
        Initializes the RawToBronze transformer with necessary configurations and utilities.
        """

        self.bundle_root_path = bundle_root_path
        self.dbutils = dbutils
        self.env = env
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
        self.raw_airbnb_adls_path = self.config["raw_airbnb_adls_path"]
        self.raw_rentals_adls_path = self.config["raw_rentals_adls_path"]
        self.raw_postcode_adls_path = self.config["raw_postcode_adls_path"]
        self.airbnb_file_name = self.config["airbnb_file_name"]
        self.rentals_file_name = self.config["rentals_file_name"]
        self.postcode_file_name = self.config["postcode_file_name"]
        self.bronze_airbnb_table = self.config["bronze_airbnb_table"]
        self.bronze_rentals_table = self.config["bronze_rentals_table"]
        self.bronze_postcode_table = self.config["bronze_postcode_table"]
        self.bronze_adls_path = self.config["bronze_adls_path"]
        self.bronze_airbnb_table_name = (
            f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_airbnb_table}"
        )
        self.bronze_rentals_table_name = (
            f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_rentals_table}"
        )
        self.bronze_postcode_table_name = f"{self.catalog_name}.{self.bronze_schema_name}.{self.bronze_postcode_table}"
        self.airbnb_raw_path = (
            self.raw_airbnb_adls_path.rstrip("/") + f"/{self.airbnb_file_name}"
        )
        self.rentals_raw_path = (
            self.raw_rentals_adls_path.rstrip("/") + f"/{self.rentals_file_name}"
        )
        self.postcode_raw_path = (
            self.raw_postcode_adls_path.rstrip("/") + f"/{self.postcode_file_name}"
        )

        self.DatabricksLogger.info(
            "Configuration loaded successfully",
            bundle_root_path=self.bundle_root_path,
            environment=self.env,
            catalog_name=self.catalog_name,
            bronze_schema_name=self.bronze_schema_name,
            raw_airbnb_adls_path=self.raw_airbnb_adls_path,
            raw_rentals_adls_path=self.raw_rentals_adls_path,
            raw_postcode_adls_path=self.raw_postcode_adls_path,
            airbnb_file_name=self.airbnb_file_name,
            rentals_file_name=self.rentals_file_name,
            postcode_file_name=self.postcode_file_name,
            bronze_adls_path=self.bronze_adls_path,
        )

        self.DatabricksLogger.info(
            "Bronze table names loaded from config",
            bronze_airbnb_table=self.bronze_airbnb_table,
            bronze_rentals_table=self.bronze_rentals_table,
            bronze_postcode_table=self.bronze_postcode_table,
        )

        self.DatabricksLogger.info(
            "Defined raw data paths",
            airbnb_raw_path=self.airbnb_raw_path,
            rentals_raw_path=self.rentals_raw_path,
            postcode_raw_path=self.postcode_raw_path,
        )

        self.DatabricksLogger.info(
            "Constructed bronze table names",
            bronze_airbnb_table_name=self.bronze_airbnb_table_name,
            bronze_rentals_table_name=self.bronze_rentals_table_name,
            bronze_postcode_table_name=self.bronze_postcode_table_name,
        )

    def run(self):
        """
        Executes the transformation logic to convert Raw data to Bronze tables.
        """

        self._process_rentals()
        self._process_airbnb()
        self._process_postcode()
        self.DatabricksLogger.info(
            "Raw to Bronze transformation completed successfully",
            airbnb_bronze_table=self.bronze_airbnb_table_name,
            rentals_bronze_table=self.bronze_rentals_table_name,
            postcode_bronze_table=self.bronze_postcode_table_name,
        )

    def _process_rentals(self):
        """
        Processes the Rentals data from the Raw layer, performs necessary transformations and cleaning, and writes the cleaned data to the Bronze layer.
        """

        renals_df = self.spark.read.json(self.rentals_raw_path)
        renals_df.write.mode("overwrite").saveAsTable(self.bronze_rentals_table_name)
        self.DatabricksLogger.info(
            f"Bronze table '{self.bronze_rentals_table_name}' created",
            row_count=renals_df.count(),
            column_count=len(renals_df.columns),
        )

    def _process_airbnb(self):
        """
        Processes the Airbnb data from the Raw layer, performs necessary transformations and cleaning, and writes the cleaned data to the Bronze layer.
        """

        airbnb_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.airbnb_raw_path)
        )
        airbnb_df.write.mode("overwrite").saveAsTable(self.bronze_airbnb_table_name)
        self.DatabricksLogger.info(
            f"Bronze table '{self.bronze_airbnb_table_name}' created",
            row_count=airbnb_df.count(),
            column_count=len(airbnb_df.columns),
        )

    def _process_postcode(self):
        """
        Processes the Postcode data from the Raw layer, performs necessary transformations and cleaning, and writes the cleaned data to the Bronze layer.
        """

        postcode_df = self.spark.read.option("multiline", "true").json(
            self.postcode_raw_path
        )
        postcode_df.write.mode("overwrite").saveAsTable(self.bronze_postcode_table_name)
        self.DatabricksLogger.info(
            f"Bronze table '{self.bronze_postcode_table_name}' created",
            row_count=postcode_df.count(),
            column_count=len(postcode_df.columns),
        )

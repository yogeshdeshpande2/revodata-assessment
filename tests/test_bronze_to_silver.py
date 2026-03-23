import pytest
import pandas as pd
from pyspark.sql import SparkSession
from bundle.src.bronze_to_silver import BronzeToSilver

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

@pytest.fixture
def setup_bronze_tables(spark):
    # Small sample data
    airbnb = pd.DataFrame({
        "zipcode": ["1234AB", None],
        "latitude": [52.1, 52.2],
        "longitude": [4.9, 4.8],
        "price": [100, 200],
        "review_scores_value": [9, 8],
        "bedrooms": [1, 2]
    })
    rentals = pd.DataFrame({
        "postalCode": ["1234AB", "5678CD"],
        "areaSqm": ["50", "60"],
        "rent": ["€ 1200", "€ 1500"]
    })
    postcode = pd.DataFrame({
        "features": [{
            "properties": {"pc4_code": "1234"},
            "geometry": {"type": "Polygon", "coordinates": [[[4.8, 52.1], [4.9, 52.1], [4.9, 52.2], [4.8, 52.2], [4.8, 52.1]]]}
        }]
    })
    spark.createDataFrame(airbnb).createOrReplaceTempView("test_catalog.bronze.bronze_airbnb")
    spark.createDataFrame(rentals).createOrReplaceTempView("test_catalog.bronze.bronze_rentals")
    spark.createDataFrame(postcode).createOrReplaceTempView("test_catalog.bronze.bronze_postcode")

def test_bronze_to_silver_run(spark, setup_bronze_tables):
    # Minimal config dict
    config = {
        "catalog_name": "test_catalog",
        "bronze_schema_name": "bronze",
        "silver_schema_name": "silver",
        "bronze_airbnb_table": "bronze_airbnb",
        "bronze_rentals_table": "bronze_rentals",
        "bronze_postcode_table": "bronze_postcode",
        "silver_airbnb_table": "silver_airbnb",
        "silver_rentals_table": "silver_rentals"
    }
    # Save config to a temp file, pass to BronzeToSilver
    # Instantiate and run BronzeToSilver, then assert on output tables
    # Example: assert spark.table("test_catalog.silver.silver_airbnb").count() == 2


# import pytest
# from unittest.mock import MagicMock, patch
# from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
# from bundle.src.bronze_to_silver import BronzeToSilver

# @pytest.fixture(scope="module")
# def spark():
#     return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

# @pytest.fixture
# def mock_dbutils():
#     dbutils = MagicMock()
#     dbutils.widgets.get.side_effect = lambda x: "/dummy/path" if x == "bundle_root_path" else "t"
#     return dbutils

# @pytest.fixture
# def mock_config(tmp_path):
#     config = {
#         "catalog_name": "test_catalog",
#         "bronze_schema_name": "bronze",
#         "silver_schema_name": "silver",
#         "bronze_airbnb_table": "bronze_airbnb",
#         "bronze_rentals_table": "bronze_rentals",
#         "bronze_postcode_table": "bronze_postcode",
#         "silver_airbnb_table": "silver_airbnb",
#         "silver_rentals_table": "silver_rentals"
#     }
#     config_path = tmp_path / "config.yaml"
#     import yaml
#     with open(config_path, "w") as f:
#         yaml.dump(config, f)
#     return str(config_path), config

# @patch("bundle.src.bronze_to_silver.Utilities")
# @patch("bundle.src.bronze_to_silver.load_config")
# @patch("bundle.src.bronze_to_silver.DatabricksLogger")
# def test_init_extracts_config_vars(mock_logger, mock_load_config, mock_utilities, mock_dbutils, mock_config):
#     config_path, config = mock_config
#     mock_load_config.return_value = config
#     mock_utilities.get_spark.return_value = MagicMock()
#     b2s = BronzeToSilver("/dummy/path", mock_dbutils, config_path, env="t")
#     assert b2s.catalog_name == config["catalog_name"]
#     assert b2s.silver_airbnb_table_name.endswith(config["silver_airbnb_table"])

# @patch("bundle.src.bronze_to_silver.BronzeToSilver._process_airbnb")
# @patch("bundle.src.bronze_to_silver.BronzeToSilver._process_rentals")
# def test_run_calls_process_methods(mock_rentals, mock_airbnb, mock_dbutils, mock_config):
#     config_path, _ = mock_config
#     b2s = BronzeToSilver("/dummy/path", mock_dbutils, config_path, env="t")
#     b2s.DatabricksLogger = MagicMock()
#     b2s.run()
#     mock_airbnb.assert_called_once()
#     mock_rentals.assert_called_once()

# @patch("bundle.src.bronze_to_silver.Utilities")
# @patch("bundle.src.bronze_to_silver.DatabricksLogger")
# def test_process_airbnb_runs_without_error(mock_logger, mock_utilities, mock_dbutils, mock_config):
#     config_path, config = mock_config
#     spark = MagicMock()
#     mock_utilities.get_spark.return_value = spark
#     b2s = BronzeToSilver("/dummy/path", mock_dbutils, config_path, env="t")
#     b2s.spark = spark
#     # Mock Spark DataFrame and SQL
#     df = MagicMock()
#     spark.read.table.return_value = df
#     df.withColumn.return_value = df
#     df.filter.return_value = df
#     df.dropna.return_value = df
#     df.dropDuplicates.return_value = df
#     df.withColumn.return_value = df
#     df.select.return_value = df
#     df.write.mode.return_value.option.return_value.saveAsTable.return_value = None
#     df.createOrReplaceTempView.return_value = None
#     spark.sql.return_value = df
#     b2s._process_airbnb()
#     assert spark.read.table.called
#     assert df.write.mode.called

# @patch("bundle.src.bronze_to_silver.Utilities")
# @patch("bundle.src.bronze_to_silver.DatabricksLogger")
# def test_process_rentals_runs_without_error(mock_logger, mock_utilities, mock_dbutils, mock_config):
#     config_path, config = mock_config
#     spark = MagicMock()
#     mock_utilities.get_spark.return_value = spark
#     b2s = BronzeToSilver("/dummy/path", mock_dbutils, config_path, env="t")
#     b2s.spark = spark
#     # Mock Spark DataFrame and SQL
#     df = MagicMock()
#     spark.read.table.return_value = df
#     df.withColumn.return_value = df
#     df.filter.return_value = df
#     df.dropna.return_value = df
#     df.dropDuplicates.return_value = df
#     df.withColumn.return_value = df
#     df.select.return_value = df
#     df.write.mode.return_value.option.return_value.saveAsTable.return_value = None
#     df.createOrReplaceTempView.return_value = None
#     b2s._process_rentals()
#     assert spark.read.table.called
#     assert df.write.mode.called

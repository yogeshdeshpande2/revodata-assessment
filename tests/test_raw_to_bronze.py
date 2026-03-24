import pytest
from pyspark.sql import SparkSession
import yaml
from bundle.src.raw_bronze import RawToBronze


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


def test_raw_to_bronze_init(spark, tmp_path):
    config = {
        "variables": {
            "t": {
                "catalog_name": "test_catalog",
                "bronze_schema_name": "bronze",
                "raw_airbnb_adls_path": "/tmp/airbnb",
                "raw_rentals_adls_path": "/tmp/rentals",
                "raw_postcode_adls_path": "/tmp/postcode",
                "airbnb_file_name": "airbnb.csv",
                "rentals_file_name": "rentals.json",
                "postcode_file_name": "postcode.json",
                "bronze_airbnb_table": "bronze_airbnb",
                "bronze_rentals_table": "bronze_rentals",
                "bronze_postcode_table": "bronze_postcode",
                "bronze_adls_path": "/tmp/bronze",
            }
        }
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    r2b = RawToBronze("/dummy/path", None, str(config_path), env="t")
    assert r2b.catalog_name == "test_catalog"

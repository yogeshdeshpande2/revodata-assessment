import pytest
import pandas as pd
from pyspark.sql import SparkSession
import yaml
from bundle.src.bronze_silver import BronzeToSilver


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture
def setup_bronze_tables(spark):
    airbnb = pd.DataFrame(
        {
            "zipcode": ["1234AB", None],
            "latitude": [52.1, 52.2],
            "longitude": [4.9, 4.8],
            "price": [100, 200],
            "review_scores_value": [9, 8],
            "bedrooms": [1, 2],
        }
    )
    rentals = pd.DataFrame(
        {
            "postalCode": ["1234AB", "5678CD"],
            "areaSqm": ["50", "60"],
            "rent": ["€ 1200", "€ 1500"],
        }
    )
    postcode = pd.DataFrame(
        {
            "features": [
                {
                    "properties": {"pc4_code": "1234"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [4.8, 52.1],
                                [4.9, 52.1],
                                [4.9, 52.2],
                                [4.8, 52.2],
                                [4.8, 52.1],
                            ]
                        ],
                    },
                }
            ]
        }
    )
    spark.createDataFrame(airbnb).createOrReplaceTempView("bronze_airbnb")
    spark.createDataFrame(rentals).createOrReplaceTempView("bronze_rentals")
    spark.createDataFrame(postcode).createOrReplaceTempView("bronze_postcode")


def test_bronze_to_silver_run(spark, tmp_path):
    config = {
        "variables": {
            "t": {
                "catalog_name": "test_catalog",
                "bronze_schema_name": "bronze",
                "silver_schema_name": "silver",
                "bronze_airbnb_table": "bronze_airbnb",
                "bronze_rentals_table": "bronze_rentals",
                "bronze_postcode_table": "bronze_postcode",
                "silver_airbnb_table": "silver_airbnb",
                "silver_rentals_table": "silver_rentals",
            }
        }
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    b2s = BronzeToSilver("/dummy/path", None, str(config_path), env="t")
    assert b2s.catalog_name == "test_catalog"

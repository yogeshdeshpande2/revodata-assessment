import pytest
from pyspark.sql import SparkSession

from bundle.src.silver_gold import SilverToGold

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

import yaml

def test_silver_to_gold_init(spark, tmp_path):
    config = {
        "variables": {
            "t": {
                "catalog_name": "test_catalog",
                "silver_schema_name": "silver",
                "gold_schema_name": "gold",
                "silver_airbnb_table": "silver_airbnb",
                "silver_rentals_table": "silver_rentals",
                "gold_top_zipcodes_airbnb": "gold_airbnb",
                "gold_top_zipcodes_rentals": "gold_rentals",
                "gold_investment_comparison": "gold_investment",
                "occupancy_rate": 0.7,
                "days_per_year": 365,
                "months_per_year": 12
            }
        }
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    s2g = SilverToGold("/dummy/path", None, str(config_path), env="t")
    assert s2g.catalog_name == "test_catalog"
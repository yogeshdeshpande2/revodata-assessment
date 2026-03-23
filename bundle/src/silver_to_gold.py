from pyspark.sql import functions as psf
from pyspark.sql.window import Window

from utils import Utilities
from utils.config_loader import load_config
from utils.databricks_logger import DatabricksLogger

class SilverToGold:
	def __init__(self, bundle_root_path, dbutils, config_path, env="t"):
		self.bundle_root_path = bundle_root_path
		self.dbutils = dbutils
		self.env = env
		self.DatabricksLogger = DatabricksLogger
		self.spark = Utilities.get_spark()
		self.config = load_config(config_path, env=env)
		self._extract_config_vars()

	def _extract_config_vars(self):
		self.catalog_name = self.config["catalog_name"]
		self.silver_schema_name = self.config["silver_schema_name"]
		self.gold_schema_name = self.config["gold_schema_name"]
		self.silver_airbnb_table = self.config["silver_airbnb_table"]
		self.silver_rentals_table = self.config["silver_rentals_table"]
		self.gold_top_zipcodes_airbnb = self.config["gold_top_zipcodes_airbnb"]
		self.gold_top_zipcodes_rentals = self.config["gold_top_zipcodes_rentals"]
		self.gold_investment_comparison = self.config["gold_investment_comparison"]
		self.occupancy_rate = self.config["occupancy_rate"]
		self.days_per_year = self.config["days_per_year"]
		self.months_per_year = self.config["months_per_year"]
		self.airbnb_silver_table_name = f"{self.catalog_name}.{self.silver_schema_name}.{self.silver_airbnb_table}"
		self.rentals_silver_table_name = f"{self.catalog_name}.{self.silver_schema_name}.{self.silver_rentals_table}"
		self.gold_top_zipcodes_airbnb_table_name = f"{self.catalog_name}.{self.gold_schema_name}.{self.gold_top_zipcodes_airbnb}"
		self.gold_top_zipcodes_rentals_table_name = f"{self.catalog_name}.{self.gold_schema_name}.{self.gold_top_zipcodes_rentals}"
		self.gold_investment_comparison_table_name = f"{self.catalog_name}.{self.gold_schema_name}.{self.gold_investment_comparison}"
		
		self.DatabricksLogger.info(
			"Configuration variables extracted",
			catalog_name=self.catalog_name,
			silver_schema_name=self.silver_schema_name,
			gold_schema_name=self.gold_schema_name,
			silver_airbnb_table=self.silver_airbnb_table,
			silver_rentals_table=self.silver_rentals_table,
			gold_top_zipcodes_airbnb=self.gold_top_zipcodes_airbnb,
			gold_top_zipcodes_rentals=self.gold_top_zipcodes_rentals,
			gold_investment_comparison=self.gold_investment_comparison,
			occupancy_rate=self.occupancy_rate,
			days_per_year=self.days_per_year,
			months_per_year=self.months_per_year
		)
						
	def run(self):
		self.DatabricksLogger.info("Starting Silver to Gold transformation")
		# self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
		# self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.silver_schema_name}")
		# self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.gold_schema_name}")
		
		self._process_airbnb()
		self._process_rentals()
		self._process_investment_comparison()
		self.DatabricksLogger.info("Silver to Gold transformation completed")

	def _process_airbnb(self):
		# spark = self.spark
		df_silver_airbnb = self.spark.table(self.airbnb_silver_table_name)
		df_gold_airbnb = (
			df_silver_airbnb
			.groupBy("postalcode4")
			.agg(
				psf.round(psf.avg("price"), 2).alias("avg_nightly_price"),
				psf.round(psf.avg("review_scores_value"), 1).alias("avg_review_score"),
				psf.round(psf.avg("bedrooms"), 1).alias("avg_bedrooms"),
				psf.count("*").alias("airbnb_listing_count")
			)
			.withColumn("est_annual_airbnb_revenue", psf.round(psf.col("avg_nightly_price") * self.days_per_year * self.occupancy_rate, 2))
		)
		window_spec = Window.orderBy(df_gold_airbnb.est_annual_airbnb_revenue.desc())
		df_gold_airbnb = df_gold_airbnb.withColumn("airbnb_revenue_rank", psf.rank().over(window_spec))
		df_gold_airbnb = df_gold_airbnb.withColumn("occupancy_rate", psf.lit(self.occupancy_rate))
		df_gold_airbnb.write.mode("append").saveAsTable(self.gold_top_zipcodes_airbnb_table_name)
		self.DatabricksLogger.info(
			f"Gold table '{self.gold_top_zipcodes_airbnb_table_name}' created",
			row_count=df_gold_airbnb.count(),
			column_count=len(df_gold_airbnb.columns)
		)
		self.df_gold_airbnb = df_gold_airbnb

	def _process_rentals(self):
		df_silver_rentals = self.spark.table(self.rentals_silver_table_name)
		df_gold_rentals = (
			df_silver_rentals
			.groupBy("postalcode4")
			.agg(
				psf.round(psf.avg("monthly_rent"), 2).alias("avg_monthly_rent"),
				psf.round(psf.avg("area_sqm"), 1).alias("avg_area_sqm"),
				psf.count("*").alias("rental_listing_count")
			)
			.withColumn("est_annual_rental_revenue", psf.round(psf.col("avg_monthly_rent") * self.months_per_year, 2))
		)
		window_spec = Window.orderBy(df_gold_rentals.est_annual_rental_revenue.desc())
		df_gold_rentals = df_gold_rentals.withColumn("rental_revenue_rank", psf.rank().over(window_spec))
		df_gold_rentals.write.mode("append").saveAsTable(self.gold_top_zipcodes_rentals_table_name)
		self.DatabricksLogger.info(
			f"Gold table '{self.gold_top_zipcodes_rentals_table_name}' created",
			row_count=df_gold_rentals.count(),
			column_count=len(df_gold_rentals.columns)
		)
		self.df_gold_rentals = df_gold_rentals

	def _process_investment_comparison(self):
		rank_window = Window.orderBy(psf.col("revenue_airbnb_minus_rentals").desc())
		investment_comparison = (
			self.df_gold_airbnb
			.join(self.df_gold_rentals, on="postalcode4", how="inner")
			.withColumn("revenue_airbnb_minus_rentals", psf.round(psf.col("est_annual_airbnb_revenue") - psf.col("est_annual_rental_revenue"), 2))
			.withColumn("revenue_ratio", psf.round(psf.col("est_annual_airbnb_revenue") / psf.col("est_annual_rental_revenue"), 2))
			.withColumn("total_listings", psf.col("airbnb_listing_count") + psf.col("rental_listing_count"))
			.withColumn("recommended_strategy",
				psf.when(psf.col("revenue_airbnb_minus_rentals") > 0, "Airbnb (Short-term)")
				.otherwise("Kamernet (Long-term)"))
			.withColumn("investment_rank", psf.rank().over(rank_window))
			.orderBy("investment_rank")
		)
		investment_comparison.write.mode("overwrite").saveAsTable(self.gold_investment_comparison_table_name)
		self.DatabricksLogger.info(
			f"Gold table '{self.gold_investment_comparison_table_name}' created",
			row_count=investment_comparison.count(),
			column_count=len(investment_comparison.columns)
		)

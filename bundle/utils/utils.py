from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

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
            spark = SparkSession.builder.getOrCreate()    
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, IntegerType

def all_sources_agg_schema_gold():
    return StructType([
        StructField("id", StringType(), True),
        StructField("app", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("iso_date", StringType(), True),
        StructField("title", StringType(), True),
        StructField("snippet", StringType(), True),
        StructField("odate", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("app_source", StringType(), True)

    ])


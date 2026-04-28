import config as c
from schemas import claims_schema
from pyspark.sql.functions import input_file_name, col

def load_data(spark):
    claims_data = spark.read.csv(c.CLAIMS_INPUT_PATH + c.CLAIMS_FILENAME_CONV, header=True, schema=claims_schema,mode="PERMISSIVE",enforceSchema=True)\
        .select("*",col("_metadata.file_name").alias("filename"))
    return claims_data
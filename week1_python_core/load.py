import os
import config as c
import pyspark.sql.functions as f
import app_logging as l

def save_data(df,table_name):
    final_path = c.OUTPUT_BASE_PATH + table_name
    os.makedirs(final_path,exist_ok=True)
    df.write.mode("overwrite").parquet(final_path)

def save_incremental_data(spark,df,table_name,data_type):
    l.log_info(f"Starting incremental load for :{data_type}")
    if data_type == "CLAIMS":
        watermark_table = c.CLAIMS_WATERMARK
    else:
        watermark_table = ''

    watermark_claims = spark.read.parquet(watermark_table)
    delta_claims = df.join(watermark_claims,how="cross")\
        .filter(f.col("claim_date") > f.col("last_claim_date"))
    claims.limit(10).display()
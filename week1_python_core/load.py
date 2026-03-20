import os
import config as c

def save_data(df,table_name):
    final_path = c.OUTPUT_BASE_PATH + table_name
    os.makedirs(final_path,exist_ok=True)
    df.write.mode("overwrite").parquet(final_path)
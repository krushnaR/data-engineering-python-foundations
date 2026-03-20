import os

def save_data(df,table_name):
    final_path = "/Volumes/data-engineering-python-foundations/default/data-engineering-python-foundations/loaded/" + table_name
    os.makedirs(final_path,exist_ok=True)
    df.write.mode("overwrite").parquet(final_path)
data = [
    ('2024-12-31')
]

cols = ["last_claim_date"]

df = spark.createDataFrame(data, cols)

df.write.mode("overwrite").parquet("/Volumes/data-engineering-python-foundations/default/data-engineering-python-foundations/watermark/claims")
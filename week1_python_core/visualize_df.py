df = spark.read.parquet("/Volumes/data-engineering-python-foundations/default/data-engineering-python-foundations/watermark/claims")

df.display(10)
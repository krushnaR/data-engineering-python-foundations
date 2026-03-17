def load_data(spark):
    claims_data = spark.read.csv("/Volumes/data-engineering-python-foundations/default/data-engineering-python-foundations/raw/fake_healthcare_claims_changed.csv", header=True)
    return claims_data
claims_data = spark.read.csv("/Volumes/data-engineering-python-foundations/default/data-engineering-python-foundations/raw/fake_healthcare_claims.csv", header=True)
claims_data.display(10)
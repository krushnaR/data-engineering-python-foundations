from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

claims_schema = StructType([
    StructField("claim_id",IntegerType(),True),
    StructField("member_id",IntegerType(),True),
    StructField("provider_id",IntegerType(),True),
    StructField("claim_date",DateType(),True),
    StructField("claim_amount",FloatType(),True),
    StructField("diagnosis_code",StringType(),True),
    StructField("status",StringType(),True)
])
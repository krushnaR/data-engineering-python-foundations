from pyspark.sql.types import StringType
from pyspark.sql import functions as f
import math

def aggregate_claims(df):

    #Get claim month
    df = df.withColumn("claim_year",f.year(df.claim_date).cast(StringType()))
    df = df.withColumn("claim_mnth",f.month(df.claim_date).cast(StringType()))

    df = df.withColumn("claim_month", f.concat(df.claim_year,f.lit("-"),f.lpad(df.claim_mnth,2,"0")))

    df = df.groupBy(df.member_id,df.claim_month).agg(f.count('claim_id').alias('claim_count'),f.sum('claim_amount').alias('total_claim_amount'))

    df_5_percent = df.groupBy(df.member_id).agg(f.sum('total_claim_amount').alias('claim_amount_member'))

    df_5_percent = df_5_percent.orderBy(f.desc("claim_amount_member"))

    total_members = df_5_percent.count()
    top_5_percent = math.ceil(total_members * 0.05)

    df_5_percent = df_5_percent.limit(top_5_percent)

    return df, df_5_percent
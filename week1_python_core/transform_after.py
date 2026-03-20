from pyspark.sql.types import StringType
from pyspark.sql import functions as f
import math

def aggregate_claims(df):

    #Get claim month
    df = df.withColumn("claim_year",f.year(df.claim_date).cast(StringType()))
    df = df.withColumn("claim_mnth",f.month(df.claim_date).cast(StringType()))

    df = df.withColumn("claim_month", f.concat(df.claim_year,f.lit("-"),f.lpad(df.claim_mnth,2,"0")))

    df = df.groupBy(df.member_id,df.claim_month).agg(f.count('claim_id').alias('claim_count'),f.sum('claim_amount').alias('total_claim_amount')).drop("claim_year","claim_mnth")

    return df

def get_high_cost_members(df):
    df_5_percent = df.groupBy(df.member_id).agg(f.sum('total_claim_amount').alias('claim_amount_member'))

    threshold = df_5_percent.agg(
        f.expr("percentile_approx(claim_amount_member, 0.95)")
    ).collect()[0][0]

    df_5_percent = df_5_percent.filter(
        f.col("claim_amount_member") >= threshold
    ).withColumn("high_cost_flag", f.lit(1))

    df = df.join(df_5_percent,on="member_id",how="left").fillna(0, subset=["high_cost_flag"]).drop("claim_amount_member")
    return df
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType

def clean_claim_dates(df):
    
    #convert date to date format
    df = df.withColumn(
        "claim_date",
        f.try_to_date(df["claim_date"],"dd-MM-yyyy")
    )

    errored_rows=df.filter(df.claim_date.isNull())
    df=df.filter(df.claim_date.isNotNull())
    return df,errored_rows

def remove_negative_claims(df):

    #move negative value claims to error
    df = df.withColumn("claim_amount",df.claim_amount.cast(FloatType()))

    errored_rows = df.filter(df.claim_amount < 0)
    df = df.filter(df.claim_amount >= 0)

    return df,errored_rows
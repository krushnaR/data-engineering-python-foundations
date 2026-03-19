from data_generation import load_data
from transformations import clean_claim_dates, remove_negative_claims
from transform_after import aggregate_claims

print("Loading claims data..")
df = load_data(spark)

print("Data Loaded Successfully!")
print(f"Total number of rows (Raw Data) : {df.count()}")

df,errored_data_dates = clean_claim_dates(df)
print(f"{errored_data_dates.count()} records found with bad dates/format")
print(f"Records after removing bad date records : {df.count()}")

df,errored_data_neg = remove_negative_claims(df)
print(f"{errored_data_neg.count()} records found with negative claim amount")
print(f"Records after removing negative claim records : {df.count()}")

agg_claims, df_5_percent = aggregate_claims(df)

agg_claims.display(10)
df_5_percent.display(10)
#errored_data_dates.display(10)
#errored_data_neg.display(10)

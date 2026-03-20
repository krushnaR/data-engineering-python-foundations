from data_generation import load_data
from transformations import clean_claim_dates, remove_negative_claims
from transform_after import aggregate_claims, get_high_cost_members
from load import save_data
from quality_checks import check_row_count, check_negative_claims, check_member_id_null
import config as c

#error handling
try:
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

    agg_claims = aggregate_claims(df)
    high_cost_members = get_high_cost_members(agg_claims)

    print("running quality checks on final dataset before loading..")
    c_row_count = check_row_count(high_cost_members)
    c_negative_claims = check_negative_claims(high_cost_members)
    c_member_id_null = check_member_id_null(high_cost_members)

    #Load the data only if all the checks passed.
    if c_row_count and c_negative_claims and c_member_id_null:
        print("Loading high cost members")
        save_data(high_cost_members,c.HIGH_COST_TABLE)
    else:
        print("Quality check failed. Data not loaded.")

except Exception as e:
    print(f"Pipeline failed {str(e)}")
    


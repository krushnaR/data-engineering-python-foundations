from data_generation import load_data
from transformations import clean_claim_dates, remove_negative_claims
from transform_after import aggregate_claims, get_high_cost_members
from load import save_data
from quality_checks import check_row_count, check_negative_claims, check_member_id_null
import config as c
from app_logging import log_info, log_error
import time

start_time = time.time()

#error handling
try:
    log_info("Loading claims data..")
    df = load_data(spark)

    log_info("Data Loaded Successfully!")
    log_info(f"Total number of rows (Raw Data) : {df.count()}")

    df,errored_data_dates = clean_claim_dates(df)
    log_error(f"{errored_data_dates.count()} records found with bad dates/format")
    log_info(f"Records after removing bad date records : {df.count()}")

    df,errored_data_neg = remove_negative_claims(df)
    log_error(f"{errored_data_neg.count()} records found with negative claim amount")
    log_info(f"Records after removing negative claim records : {df.count()}")

    save_data(errored_data_dates,"errored_data_dates")
    save_data(errored_data_neg,"errored_data_neg")

    agg_claims = aggregate_claims(df)
    high_cost_members = get_high_cost_members(agg_claims)

    log_info("running quality checks on final dataset before loading..")
    c_row_count = check_row_count(high_cost_members)
    c_negative_claims = check_negative_claims(high_cost_members)
    c_member_id_null = check_member_id_null(high_cost_members)

    #Load the data only if all the checks passed.
    if c_row_count and c_negative_claims and c_member_id_null:
        log_info("Loading high cost members")
        save_data(high_cost_members,c.HIGH_COST_TABLE)
    else:
        log_error("Quality check failed. Data not loaded.")

except Exception as e:
    log_error(f"Pipeline failed {str(e)}")

end_time = time.time()
log_info(f"Total time taken by pipeline : {end_time - start_time} seconds")    


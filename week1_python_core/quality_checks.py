from app_logging import log_info, log_error
from pyspark.sql import functions as f

def check_row_count(df):
    count = df.count()

    if count == 0:
        log_error("Final Dataset is empty!")
        return False
    else:
        log_info(f"Total record count in final dataset:{count}")
        return True

def check_negative_claims(df):
    df = df.filter(df["total_claim_amount"] < 0)
    count = df.count()

    if count == 0:
        log_info("No negative claim amounts found")
        return True
    else:
        log_error(f"{count} number of records found with negative claim amounts")
        return False

def check_member_id_null(df):
    df = df.filter(f.isnull(df["member_id"]))
    count = df.count()

    if count == 0:
        log_info("All member ids are valid.")
        return True
    else:
        log_error(f"{count} number of records found with null member ids")
        return False
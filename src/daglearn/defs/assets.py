import pandas as pd
import dagster as dg


RAW_FILE = "src/daglearn/defs/data/raw/orders.csv"
PROCESSED_FILE = "src/daglearn/defs/data/transformed/orders_summary.csv"


@dg.asset
def transform():

    # Read data
    df = pd.read_csv(RAW_FILE)

    # Group sales by shipping city
    df = (
        df.groupby("shipping_city", as_index=False)["total_amount"]
        .sum()
        .rename(columns={"total_amount": "amount"})
    )

    # Save processed data
    df.to_csv(PROCESSED_FILE, index=False)

    return "Data aggregated successfully."

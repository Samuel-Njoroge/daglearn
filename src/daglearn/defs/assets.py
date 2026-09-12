from dagster_duckdb import DuckDBResource

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


@dg.asset
def orders(duckdb: DuckDBResource):

    file_path = "src/daglearn/defs/data/raw/orders.csv"
    table_name = "orders"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS (
            SELECT * FROM read_csv_auto('{file_path}')
            )
            """
        )


@dg.asset
def customers(duckdb: DuckDBResource):

    file_path = "src/daglearn/defs/data/raw/customers.csv"
    table_name = "customers"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS (
            SELECT * FROM read_csv_auto('{file_path}')
            )
            """
        )




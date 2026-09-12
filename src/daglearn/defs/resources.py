from dagster_duckdb import DuckDBResource

import dagster as dg


db = DuckDBResource(database="../warehouse.duckdb")


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": db,
        }
    )

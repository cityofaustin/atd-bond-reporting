import pandas as pd
from config.csv_config import CSVS

import os
from pypgrest import Postgrest

POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")


def field_mapping(df, maps):
    """
    Renames columns in a dataframe to match the schema of the corresponding postgrest table

    Parameters
    ----------
    df: Pandas dataframe of the raw data with original columns
    maps: dict of field mappings between CSV and table columns

    Returns: df with renamed columns
    -------
    """
    cols = df.columns

    # Making sure that all columns are present
    for column in cols:
        assert column in maps

    df = df.rename(columns=maps)
    return df


def convert_datetime(df, col):
    df[col] = pd.to_datetime(df[col], format="%m/%d/%Y").astype(str)
    return df


def validate_schema(df, schema):
    """
    Compare our df's schema to the schema expected by the postgres table.

    Parameters
    ----------
    df: Pandas dataframe that you are validating
    schema: pandera DataFrameSchema object for the provided table

    Returns: The same df back or will raise an error if it does not align with schema
    -------

    """
    df = schema.validate(df)
    return df


def to_postgres(client, df, table):
    # Creating updated_at column
    time = pd.to_datetime("now")
    df["updated_at"] = str(time)

    # Send df to database
    payload = df.to_dict(orient="records")
    try:
        # Insert updated data
        res = client.insert(resource=table, data=payload)
        # Now delete outdated data
        params = {"select": "*", "updated_at": f"lt.{time}", "order": "updated_at"}
        res = client.delete(resource=table, params=params)
    except Exception as e:
        raise e
    return res


def main():
    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    for table in CSVS:
        df = pd.read_csv(table["url"])
        df = field_mapping(df, table["field_maps"])
        if table["date_field"]:
            df = convert_datetime(df, "date")
        df = validate_schema(df, table["schema"])
        res = to_postgres(client, df, table["table"])


if __name__ == "__main__":
    main()

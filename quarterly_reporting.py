import numpy as np
import pandas as pd
from pypgrest import Postgrest
from sodapy import Socrata

import os

POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")

# Socrata dataset IDs
SUBPROJECT_DATASET = os.getenv("SUBPROJECT_DATASET")
FDU_DATASET = os.getenv("FDU_DATASET")
EXPENSES_DATASET = os.getenv("EXPENSES_DATASET")

SUBPROJECT_TABLES = [
    "fdus_2020_bond",
    "subprojects_with_appropriations",
    "subprojects_with_budget",
]
EXPENSES = "fdu_expenses_quarterly"
SPEND_PLAN = "quarterly_spend_plan"
FDU_METADATA = "fdu_metadata_quarterly"



FY_MONTHS = {
    "OCT": "00",
    "NOV": "01",
    "DEC": "02",
    "JAN": "03",
    "FEB": "04",
    "MAR": "05",
    "APR": "06",
    "MAY": "07",
    "JUN": "08",
    "JUL": "09",
    "AUG": "10",
    "SEP": "11",
    "P13": "11",  # Putting period 13 in September
}

FY_MONTH_DECODED = {
    "00": "October",
    "01": "November",
    "02": "December",
    "03": "January",
    "04": "February",
    "05": "March",
    "06": "April",
    "07": "May",
    "08": "June",
    "09": "July",
    "10": "August",
    "11": "September",
}
FY_MONTH_DECODED_INVERSE = {v: k for k, v in FY_MONTH_DECODED.items()}


def get_data(client, table):
    """
    Gets all data from postgrest endpoint and returns it as a dataframe.
    """
    params = {
        "select": "*",
        "order": "updated_at",
    }
    return pd.DataFrame(client.select(resource=table, params=params))


def subproject_transformation(client):
    """
    Combines subproject reports into one dataframe
    Parameters
    ----------
    client: Postgrest client

    Returns
    -------
    df of combined subproject reports

    """

    df = get_data(client, SUBPROJECT_TABLES[0])
    df = df.set_index("subproject_number")
    # Merge the rest into the starter df
    for table in SUBPROJECT_TABLES[1:3]:
        df2 = pd.DataFrame(get_data(client, table))
        df2 = df2.set_index("subproject_number")
        cols_to_use = df2.columns.difference(df.columns)
        df = df.merge(df2[cols_to_use], left_index=True, right_index=True, how="left")

    return df


def create_fdu_metadata_table(client):
    """
    Returns a df with the metadata for each FDU/subproject, with some transformations.
    """

    # Formatting metadata table
    metadata = pd.DataFrame(get_data(client, FDU_METADATA))
    # For bikeways group all subprograms together
    metadata["subprogram_long_name"] = np.where(
        metadata["program_code"] == "D001",
        metadata["program_long_name"],
        metadata["subprogram_long_name"],
    )
    # If ZZZZ for subprogram, use the program code as well
    metadata["subprogram_long_name"] = np.where(
        metadata["subprogram_code"] == "ZZZZ",
        metadata["program_long_name"],
        metadata["subprogram_long_name"],
    )
    return metadata


def create_month_col(row):
    # Gets the fiscal month of the row and the FY and concatenates them
    return f"{row['fiscal_year']}{FY_MONTHS[row['month-year'][0:3]]}"


def select_quarter(row, quarter):
    # Sets "fy-quarter" to the month_col for the selected quarter and everything else to zero.
    if row["fy-quarter"] == quarter:
        return row["month_col"]
    return "0"


def decode_months(row):
    # Returns the name of the month based on the month_col
    if row["month_col"] == "0":
        return "Prior Spend"
    return FY_MONTH_DECODED[row["month_col"][-2:]]


def encode_months(row):
    # encodes month to the numeric fiscal month value
    return FY_MONTH_DECODED_INVERSE[row["month"]]


def to_output(df, spend):
    # Prepares the df for export by appending the spend plan info and calculating cumulative totals
    # Melt the pivot table back down so the month columns become rows
    df = pd.melt(
        df.reset_index(),
        id_vars="unit_code",
        var_name="month_col",
        value_name="expenses",
    )
    # merge the spend plan data in on the month_col
    df = pd.merge(df, spend, on=["unit_code", "month_col"], how="left")
    # Get the text month name, all previous months to the quarter get grouped as Prior Spend
    df["Month Name"] = df.apply(decode_months, axis=1)
    # Setting the start of the spend plan equal to the total of past expenses
    df["spend_plan"] = np.where(
        df["Month Name"] == "Prior Spend", df["expenses"], df["spend_plan"]
    )

    # Cumulative totals for the quarter for the three months
    # Pulling the monthly expense data out for now
    quarter = df[df["month_col"] != "0"]
    quarter["spend_plan"] = quarter["spend_plan"].fillna(0)
    quarter = quarter.set_index("month_col")
    quarter = quarter.sort_index()
    # Then calculate the cumulative total by FDU by month
    quarter["sum_expenses"] = quarter.groupby(["unit_code"])["expenses"].cumsum()
    quarter["sum_planned"] = quarter.groupby(["unit_code"])["spend_plan"].cumsum()
    quarter = quarter.reset_index()

    # Then append the monthly expense data back in
    df = df[df["month_col"] == "0"]
    df = df.append(quarter)

    return df

def df_to_socrata(soda, df, dataset_id, include_index):
    if include_index:
        df = df.reset_index()
    df = df.replace({np.nan: None})
    payload = df.to_dict(orient="records")
    try:
        res = soda.replace(dataset_id, payload)
    except Exception as e:
        raise e


def main():
    # Postgrest client log in
    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )
    # Socrata client log in
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=500, )

    # Download & transform subproject reports
    # subprojects = subproject_transformation(client)
    # df_to_socrata(soda, subprojects, SUBPROJECT_DATASET, True)

    # Download & transform subproject reports
    metadata = create_fdu_metadata_table(client)
    # unit_code is used to join in the spend plan data, but after TPW merger we need to include department as well.
    metadata["unit_code"] = metadata["department"].astype(str) + metadata["unit_code"]
    metadata.drop(["department", "department_long_name"], axis=1, inplace=True)
    df_to_socrata(soda, metadata, FDU_DATASET, False)

    # Get expenses data by FDU
    exp = get_data(client, EXPENSES)

    # Now, create a column we will use to group by later of the form FY-MON
    exp["month_col"] = exp.apply(create_month_col, axis=1)

    # unit_code is used to join in the spend plan data, but after TPW merger we need to include department as well.
    exp["unit_code"] = exp["department"].astype(str) + exp["unit_code"]

    # Group by FDU, and calculate the expenses by each month
    exp = pd.pivot_table(
        exp, index="unit_code", columns="month_col", values="expenses", aggfunc="sum"
    )
    exp = exp.fillna(0)

    # Now, unpivot the columns so that each row is a month for an FDU
    # from: FDU, 22-OCT, 22-NOV, 22-DEC, ...
    # to: FDU, month, expenses
    exp = pd.melt(
        exp.reset_index(),
        id_vars="unit_code",
        var_name="month_col",
        value_name="expenses",
    )

    # Calculating fiscal quarter column based on the month
    exp["quarter"] = exp["month_col"].str[-2:].astype(int) // 3 + 1

    # Extract FY data from month_col
    exp["fiscal_year"] = exp["month_col"].str[:4].astype(int)

    # concat the fiscal year to the quarter number
    exp["fy-quarter"] = exp["fiscal_year"].astype(str) + exp["quarter"].astype(str)

    # Calculate the current quarter (assumed latest)
    curr_quarter = exp["fy-quarter"].max()

    # Returns a column where we check if each row is the current quarter
    exp["curr_quarter"] = exp.apply(select_quarter, axis=1, args=[curr_quarter])

    # calculating previous quarter, if it's Q1 then prev is Q4 and all others
    if int(curr_quarter[-1:]) > 1:
        prev_quarter = f"{curr_quarter[:4]}{int(curr_quarter[-1:]) - 1}"
    else:
        prev_quarter = f"{int(curr_quarter[:4]) - 1}{3}"

    # Extract only the previous quarter's data for the previous quarter tab
    prev_exp = exp[exp["fy-quarter"] <= prev_quarter]

    # Make a pivot table where each row is an FDU and the columns are either 0 or the month of the selected quarter
    # Like: FDU, 0, Jan-2023, Feb-2023, Mar-2023
    # 0 is the previous expenses leading up to the quarter
    exp = pd.pivot_table(
        exp, index="unit_code", columns="curr_quarter", values="expenses", aggfunc="sum"
    )

    # Adding missing months from selected quarter
    # if we only have a one or two months of data in the quarter we have to manually add zero expenses for them to
    # show up.
    if len(exp.columns) < 4:
        for m in range(4 - len(exp.columns)):
            latest_month = exp.columns.max()
            added_month = str(int(latest_month[-2:]) + 1)
            if len(added_month) == 1:
                exp[f"{latest_month[:-2]}0{added_month}"] = 0
            else:
                exp[f"{latest_month[:-2]}{added_month}"] = 0

    # Now, do something similar for the previous quarter
    prev_exp["prev_quarter"] = prev_exp.apply(
        select_quarter, axis=1, args=[prev_quarter]
    )
    prev_exp = pd.pivot_table(
        prev_exp,
        index="unit_code",
        columns="prev_quarter",
        values="expenses",
        aggfunc="sum",
    )

    # Gather spend plan data (originally in Google Drive)
    spend_plan = get_data(client, SPEND_PLAN)
    # unit_code is used to join in the spend plan data, but after TPW merger we need to include department as well.
    spend_plan["unit_code"] = spend_plan["department"].astype(str) + spend_plan["unit_code"]

    spend_plan["month_no"] = spend_plan.apply(encode_months, axis=1)
    # Make the same month_col, so we can match it back to the expenses data
    spend_plan["month_col"] = (
        spend_plan["fiscal_year"].astype(str) + spend_plan["month_no"]
    )

    # Merging the spend plan into the expenses for current quarter
    exp = to_output(exp, spend_plan)
    # Creating column for selecting the quarter in Power BI
    exp["quarter_selection"] = f"{curr_quarter[:4]} Q{curr_quarter[-1:]}"

    # Merging the spend plan into the expenses for previous quarter
    prev_exp = to_output(prev_exp, spend_plan)
    # Creating column for selecting the quarter in Power BI
    prev_exp["quarter_selection"] = f"{prev_quarter[:4]} Q{prev_quarter[-1:]}"

    # Append the previous expenses to the current expenses
    output = exp.append(prev_exp)

    # Replace data in Socrata
    output.drop(["department"], axis=1, inplace=True)
    df_to_socrata(soda, output, EXPENSES_DATASET, False)


if __name__ == "__main__":
    main()

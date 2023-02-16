from io import StringIO
import os

import pandas as pd
import boto3


# AWS Credentials
AWS_ACCESS_ID = os.getenv("BOND_AWS_ID")
AWS_PASS = os.getenv("BOND_AWS_SECRET")
BUCKET = os.getenv("BOND_BUCKET")

## CSV Endpoints
# Microstrategy
# 2020 Bond Expenses Obligated.csv
BOND_2020_EXP = "https://atd-microstrategy-reports.s3.amazonaws.com/2020+Bond+Expenses+Obligated.csv"
# All bonds Expenses Obligated.csv
ALL_BONDS_EXP = "https://atd-microstrategy-reports.s3.amazonaws.com/All+bonds+Expenses+Obligated.csv"

# Google Docs:
# AIMS to Dashboard ID Lookup table
AIMS_DASHBOARD = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSGZb6KRHwiSgxxsIrmxzDtEFR8Dg1QFfQ65dybwBv_EvZRCh3Fi1YqOP3vYI1uOe8M5ZZVFVuvUkZ-/pub?gid=202806724&single=true&output=csv"
# Baseline Spend data
BASELINE_SPEND = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSGZb6KRHwiSgxxsIrmxzDtEFR8Dg1QFfQ65dybwBv_EvZRCh3Fi1YqOP3vYI1uOe8M5ZZVFVuvUkZ-/pub?output=csv"
# Current FY Spend Plan
CY_SPEND_PLAN = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSGZb6KRHwiSgxxsIrmxzDtEFR8Dg1QFfQ65dybwBv_EvZRCh3Fi1YqOP3vYI1uOe8M5ZZVFVuvUkZ-/pub?gid=1856896414&single=true&output=csv"
# Previous FY Spend Plan
PY_SPEND_PLAN = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSGZb6KRHwiSgxxsIrmxzDtEFR8Dg1QFfQ65dybwBv_EvZRCh3Fi1YqOP3vYI1uOe8M5ZZVFVuvUkZ-/pub?gid=1042748786&single=true&output=csv"
# Dashboard Appropriation for all bonds
APPRO_DATA = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhT8BEVrEMi3ISFPiz8ujKqmIkBgX9kvEQCdTU3eneG46cCr1-Cf1KJ5wovsej6gNPYx9UBEGN4VKi/pub?gid=20835862&single=true&output=csv"
# All bonds DeptFundProgAct lookup table
DEPTFUNDPROGACT = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhT8BEVrEMi3ISFPiz8ujKqmIkBgX9kvEQCdTU3eneG46cCr1-Cf1KJ5wovsej6gNPYx9UBEGN4VKi/pub?gid=1889713154&single=true&output=csv"
# All bonds AIMS to dashboard ID lookup table
AIMS_ALL_BONDS_DASHBOARD = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhT8BEVrEMi3ISFPiz8ujKqmIkBgX9kvEQCdTU3eneG46cCr1-Cf1KJ5wovsej6gNPYx9UBEGN4VKi/pub?gid=811620554&single=true&output=csv"


# Used for converting numeric months into sortable strings in Power BI
MONTH_NAMES = {
    1: "01",
    2: "02",
    3: "03",
    4: "04",
    5: "05",
    6: "06",
    7: "07",
    8: "08",
    9: "09",
    10: "10",
    11: "11",
    12: "12",
}


def df_to_s3(df, resource, filename, index):
    """
    Send pandas dataframe to an S3 bucket as a CSV
    h/t https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python

    Parameters
    ----------
    df : Pandas Dataframe
    resource : boto3 s3 resource
    filename : String of the file that will be created in the S3 bucket
    index: boolean that will or will not publish the dataframe index in the csv

    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=index)
    resource.Object(BUCKET, f"{filename}.csv").put(Body=csv_buffer.getvalue())


def expenses_obligated(df):
    """
    Generates the cumulative sum of the expenses and obligation data

    Parameters
    ----------
    df : Pandas dataframe
        Expenses and obligation data for the 2020 bond.

    Returns
    -------
    Processed data with cumulative sum columns.

    df : Pandas dataframe
        Current FY
    pdf : Pandas dataframe
        Previous FY

    """

    # Lookup column we use is a concatenation of a few fields
    df["AIMS DeptFundProgAct"] = (
        df["Department"].astype(str) + df["Fund"] + df["Division"] + df["Group"]
    )

    # Here, we are creating new rows in this dataset for the missing rows
    # One row per FY, date, and AIMS DeptFundProgAct
    # If we don't do this then the cumulative totals when summed will not be correct
    fys = df["Fiscal Year"].unique()
    # Current fiscal year is determined based in the latest in our data
    curr_year = fys.max()

    dates = df["Date"].unique()
    groups = df["AIMS DeptFundProgAct"].unique()
    new_rows = []

    for fy in fys:
        for date in dates:
            for group in groups:
                if group not in list(
                    df[(df["Date"] == date) & (df["Fiscal Year"] == fy)][
                        "AIMS DeptFundProgAct"
                    ]
                ):
                    row = {
                        "Fiscal Year": fy,
                        "AIMS DeptFundProgAct": group,
                        "Date": date,
                        "Expenses": 0,
                        "Obligated": 0,
                    }
                    new_rows.append(row)

    new_rows = pd.DataFrame(new_rows)
    df = pd.concat([df, new_rows], ignore_index=True)

    # Creating datetime index and sorting ascending by that
    df["datetime"] = pd.to_datetime(df["Date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # Cumulative sum is what is plotted in Power BI, we create a rolling total
    # for each AIMS DeptFundProgAct and FY
    df["sum_obligated"] = df.groupby(["AIMS DeptFundProgAct", "Fiscal Year"])[
        "Obligated"
    ].cumsum()

    df["sum_expenses"] = df.groupby(["AIMS DeptFundProgAct", "Fiscal Year"])[
        "Expenses"
    ].cumsum()

    # Export two versions, one for current FY and one for previous FY
    pdf = df[df["Fiscal Year"] < curr_year]

    return df, pdf


def all_bond_expenses_obligated(df, app):
    # Lookup column we use is a concatenation of a few fields
    df["AIMS DeptFundProgAct"] = (
        df["Department@Dept"].astype(str)
        + df["Fund@Code"]
        + df["Division (As-Is)@Code"]
        + df["Group (As-Is)@Code"]
    )

    # Here, we are creating new rows in this dataset for the missing rows
    # One row per date and AIMS DeptFundProgAct
    # If we don't do this then the cumulative totals when summed will not be correct

    dates = df["Date"].unique()
    dates = pd.DataFrame(dates)
    dates = dates.rename(columns={0: "Date"})

    groups = df["AIMS DeptFundProgAct"].unique()
    groups = pd.DataFrame(groups)
    groups = groups.rename(columns={0: "AIMS DeptFundProgAct"})

    new_rows = dates.merge(groups, how="cross")
    new_rows["Expenses"] = 0
    new_rows["Obligated"] = 0
    df = pd.concat([df, new_rows], ignore_index=True)
    df = df.drop_duplicates(subset=["AIMS DeptFundProgAct", "Date"], keep="first")

    # Creating datetime index and sorting ascending by that
    df["datetime"] = pd.to_datetime(df["Date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # Cumulative sum is what is plotted in Power BI, we create a rolling total
    # for each AIMS DeptFundProgAct and FY
    df["sum_obligated"] = df.groupby(["AIMS DeptFundProgAct"])["Obligated"].cumsum()

    df["sum_expenses"] = df.groupby(["AIMS DeptFundProgAct"])["Expenses"].cumsum()

    return df


def fiscal_year(row):
    # Calculates city fiscal year for a given month/year
    if row["group"][1] > 9:
        return row["group"][0] + 1
    return row["group"][0]


def group_table(row, fiscal_year):
    if row["group"][3] == fiscal_year and row["date-fy"] == fiscal_year:
        return f"{row['group'][0]} {MONTH_NAMES[row['group'][1]]}"
    elif row["group"][3] == fiscal_year and row["date-fy"] > fiscal_year:
        return f"{str(fiscal_year)} 09"
    elif row["group"][3] == fiscal_year and row["date-fy"] < fiscal_year:
        return f"{str(fiscal_year-1)} 10"
    return f"0FY {str(row['group'][3])[2:4]}"


def summarize_expenses(df,fy):

    # Need to convert from DeptFundProgAct to Dashboard DeptFundProgAct first
    # AIMS -> Dashboard ID lookup table
    xwalk = pd.read_csv(AIMS_DASHBOARD)
    xwalk = xwalk.rename(columns={"AIMS Dept Prog Act": "AIMS DeptFundProgAct"})

    df = pd.merge(df, xwalk, on="AIMS DeptFundProgAct", how="left")

    # Setting datetime index again
    df["datetime"] = pd.to_datetime(df["Date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # We are going to summarize based on the max fiscal year
    #fy = df["Fiscal Year"].max()

    # Summarizing our expenses data by year, month, Dashboard DeptFundProgAct, and FY
    df = df.groupby(
        [df.index.year, df.index.month, "Dashboard DeptFundProgAct", "Fiscal Year"]
    ).sum()

    # Creates a group column that allows us to access it inside other functions
    df["group"] = df.index.to_series()

    # Get the Fiscal year for the date in each row
    df["date-fy"] = df.apply(fiscal_year, axis=1)

    # table_col is the column with the month or FY we will summarize by later
    df["table_col"] = df.apply(group_table, fiscal_year=fy, axis=1)

    return df


# don't have to worry about the fiscal year in this function
def group_plans(row, fiscal_year):
    if row["Fiscal Year"] == fiscal_year:
        return f"{row['group'][0]} {MONTH_NAMES[row['group'][1]]}"
    return f"0FY {str(row['Fiscal Year'])[2:4]}"

def determine_fy():
    # Looks at the current year spend plan and returns the maximum fiscal year
    df = pd.read_csv(CY_SPEND_PLAN)
    df["datetime"] = pd.to_datetime(df["Date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    df = df.groupby([df.index.year, df.index.month, "Dashboard DeptFundProgAct"]).sum()

    df["group"] = df.index.to_series()

    df["Fiscal Year"] = df.apply(fiscal_year, axis=1)
    return df["Fiscal Year"].max()


def summarize_plans(file, fy):
    df = pd.read_csv(file)

    df["datetime"] = pd.to_datetime(df["Date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    df = df.groupby([df.index.year, df.index.month, "Dashboard DeptFundProgAct"]).sum()

    df["group"] = df.index.to_series()

    df["Fiscal Year"] = df.apply(fiscal_year, axis=1)

    df["table_col"] = df.apply(group_plans, fiscal_year=fy, axis=1)
    df = df.groupby(["table_col", "Dashboard DeptFundProgAct"]).sum()
    return df

def summary_table(expenses, fiscal_year):
    dfs = []
    for i in range(-1, 1):
        fy = fiscal_year + i
        if i == -1:
            spend_plan = PY_SPEND_PLAN
        else:
            spend_plan = CY_SPEND_PLAN

        expenses_summary = summarize_expenses(expenses, fy)
        expenses_summary = expenses_summary.groupby(["table_col", "Dashboard DeptFundProgAct"]).sum()

        baseline_summary = summarize_plans(BASELINE_SPEND, fy)
        baseline_summary = baseline_summary.rename(columns={"Amount": "Baseline"})

        spend_summary = summarize_plans(spend_plan, fy)
        spend_summary = spend_summary.rename(columns={"Amount": "Planned"})

        output = baseline_summary.join(expenses_summary, lsuffix="_x", rsuffix="_y")
        output = output.join(spend_summary, lsuffix="_x", rsuffix="_y")

        output = output[["Expenses", "Baseline", "Planned"]]
        if i == -1:
            output = output[output.index.get_level_values('table_col') != f"0FY {fiscal_year - 2000}"]

        output["Sum_expenses"] = output.groupby(["Dashboard DeptFundProgAct"])[
            "Expenses"
        ].cumsum()
        output["Sum_baseline"] = output.groupby(["Dashboard DeptFundProgAct"])[
            "Baseline"
        ].cumsum()
        output["Sum_planned"] = output.groupby(["Dashboard DeptFundProgAct"])[
            "Planned"
        ].cumsum()

        dfs.append(output)

    return dfs[0], dfs[1]




def main():
    # Data from Microstrategy is in S3
    # 2020 Bond Expenses Obligated.csv
    bond_data_2020 = pd.read_csv(BOND_2020_EXP)
    bond_data_2020, py_bond_data_2020 = expenses_obligated(bond_data_2020)

    all_bond_data = pd.read_csv(ALL_BONDS_EXP)
    app = pd.read_csv(APPRO_DATA)

    all_bond_data = all_bond_expenses_obligated(all_bond_data, app)

    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )

    df_to_s3(bond_data_2020, s3_resource, "curr_fyear_obligated_expenses", False)
    df_to_s3(py_bond_data_2020, s3_resource, "prev_fyear_obligated_expenses", False)
    df_to_s3(all_bond_data, s3_resource, "all_bonds_obligation_expenses", False)

    fy = determine_fy()

    py_summary, cy_summary = summary_table(bond_data_2020, fy)
    df_to_s3(cy_summary, s3_resource, "curr_year_table", True)
    df_to_s3(py_summary, s3_resource, "prev_year_table", True)


if __name__ == "__main__":
    main()

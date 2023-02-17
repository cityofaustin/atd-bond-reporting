from io import StringIO
import os

import pandas as pd
import boto3
from pypgrest import Postgrest
# AWS Credentials
AWS_ACCESS_ID = os.getenv("BOND_AWS_ID")
AWS_PASS = os.getenv("BOND_AWS_SECRET")
BUCKET = os.getenv("BOND_BUCKET")

# Postgest Credentials
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")

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

def get_data(client, table):
    """

    Parameters
    ----------
    client - Postgrest client
    table - Name of the table in the Postgrest database

    Returns
    -------
    A pandas dataframe of the whole table

    """
    params = {"select": "*", "order": "updated_at"}
    res = client.select(resource=table, params=params)
    return pd.DataFrame(res)


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
    df["aims_dept_prog_act"] = (
        df["department"].astype(str) + df["fund"] + df["division"] + df["group"]
    )

    # Here, we are creating new rows in this dataset for the missing rows
    # One row per FY, date, and AIMS DeptFundProgAct
    # If we don't do this then the cumulative totals when summed will not be correct
    fys = df["fiscal_year"].unique()
    # Current fiscal year is determined based in the latest in our data
    curr_year = fys.max()

    dates = df["date"].unique()
    groups = df["aims_dept_prog_act"].unique()
    new_rows = []

    for fy in fys:
        for date in dates:
            for group in groups:
                if group not in list(
                    df[(df["date"] == date) & (df["fiscal_year"] == fy)][
                        "aims_dept_prog_act"
                    ]
                ):
                    row = {
                        "fiscal_year": fy,
                        "aims_dept_prog_act": group,
                        "date": date,
                        "expenses": 0,
                        "obligated": 0,
                    }
                    new_rows.append(row)

    new_rows = pd.DataFrame(new_rows)
    df = pd.concat([df, new_rows], ignore_index=True)

    # Creating datetime index and sorting ascending by that
    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # Cumulative sum is what is plotted in Power BI, we create a rolling total
    # for each AIMS DeptFundProgAct and FY
    df["sum_obligated"] = df.groupby(["aims_dept_prog_act", "fiscal_year"])[
        "obligated"
    ].cumsum()

    df["sum_expenses"] = df.groupby(["aims_dept_prog_act", "fiscal_year"])[
        "expenses"
    ].cumsum()

    # Export two versions, one for current FY and one for previous FY
    pdf = df[df["fiscal_year"] < curr_year]

    return df, pdf


def all_bond_expenses_obligated(df, app):
    # Lookup column we use is a concatenation of a few fields
    df["aims_dept_prog_act"] = (
        df["department"].astype(str)
        + df["fund_code"]
        + df["division_code"]
        + df["group_code"]
    )

    # Here, we are creating new rows in this dataset for the missing rows
    # One row per date and AIMS DeptFundProgAct
    # If we don't do this then the cumulative totals when summed will not be correct

    dates = df["date"].unique()
    dates = pd.DataFrame(dates)
    dates = dates.rename(columns={0: "date"})

    groups = df["aims_dept_prog_act"].unique()
    groups = pd.DataFrame(groups)
    groups = groups.rename(columns={0: "aims_dept_prog_act"})

    new_rows = dates.merge(groups, how="cross")
    new_rows["expenses"] = 0
    new_rows["obligated"] = 0
    df = pd.concat([df, new_rows], ignore_index=True)
    df = df.drop_duplicates(subset=["aims_dept_prog_act", "date"], keep="first")

    # Creating datetime index and sorting ascending by that
    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # Cumulative sum is what is plotted in Power BI, we create a rolling total
    # for each AIMS DeptFundProgAct and FY
    df["sum_obligated"] = df.groupby(["aims_dept_prog_act"])["obligated"].cumsum()

    df["sum_expenses"] = df.groupby(["aims_dept_prog_act"])["expenses"].cumsum()

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


def summarize_expenses(df, fy, client):

    # Need to convert from DeptFundProgAct to Dashboard DeptFundProgAct first
    # AIMS -> Dashboard ID lookup table
    xwalk = get_data(client, "bond_2020_aims_to_dashboard")
    #xwalk = xwalk.rename(columns={"AIMS Dept Prog Act": "AIMS DeptFundProgAct"})

    df = pd.merge(df, xwalk, on="aims_dept_prog_act", how="left")

    # Setting datetime index again
    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    # Summarizing our expenses data by year, month, Dashboard DeptFundProgAct, and FY
    df = df.groupby(
        [df.index.year, df.index.month, "dashboard_deptfundprogact", "fiscal_year"]
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
    if row["fiscal_year"] == fiscal_year:
        return f"{row['group'][0]} {MONTH_NAMES[row['group'][1]]}"
    return f"0FY {str(row['fiscal_year'])[2:4]}"

def determine_fy(client):
    # Looks at the current year spend plan and returns the maximum fiscal year
    df = get_data(client, "bond_2020_current_fy_spend_plan")
    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    df = df.groupby([df.index.year, df.index.month, "dashboard_deptfundprogact"]).sum()

    df["group"] = df.index.to_series()

    df["fiscal_year"] = df.apply(fiscal_year, axis=1)
    return df["fiscal_year"].max()


def summarize_plans(file, fy, client):
    df = get_data(client, file)

    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    df = df.sort_index()

    df = df.groupby([df.index.year, df.index.month, "dashboard_deptfundprogact"]).sum()

    df["group"] = df.index.to_series()

    df["fiscal_year"] = df.apply(fiscal_year, axis=1)

    df["table_col"] = df.apply(group_plans, fiscal_year=fy, axis=1)
    df = df.groupby(["table_col", "dashboard_deptfundprogact"]).sum()
    return df

def summary_table(expenses, fiscal_year,client):
    dfs = []
    for i in range(-1, 1):
        fy = fiscal_year + i
        if i == -1:
            spend_plan = "bond_2020_previous_fy_spend_plan"
        else:
            spend_plan = "bond_2020_current_fy_spend_plan"

        expenses_summary = summarize_expenses(expenses, fy, client)
        expenses_summary = expenses_summary.groupby(["table_col", "dashboard_deptfundprogact"]).sum()
        expenses_summary = expenses_summary.rename(columns={"expenses": "Expenses"})

        baseline_summary = summarize_plans("bond_2020_baseline_spend", fy, client)
        baseline_summary = baseline_summary.rename(columns={"amount": "Baseline"})

        spend_summary = summarize_plans(spend_plan, fy, client)
        spend_summary = spend_summary.rename(columns={"amount": "Planned"})

        output = baseline_summary.join(expenses_summary, lsuffix="_x", rsuffix="_y",how='outer')
        output = output.join(spend_summary, lsuffix="_x", rsuffix="_y")

        output = output[["Expenses", "Baseline", "Planned"]]
        if i == -1:
            output = output[output.index.get_level_values('table_col') != f"0FY {fiscal_year - 2000}"]

        output["Sum_expenses"] = output.groupby(["dashboard_deptfundprogact"])[
            "Expenses"
        ].cumsum()
        output["Sum_baseline"] = output.groupby(["dashboard_deptfundprogact"])[
            "Baseline"
        ].cumsum()
        output["Sum_planned"] = output.groupby(["dashboard_deptfundprogact"])[
            "Planned"
        ].cumsum()

        dfs.append(output)

    return dfs[0], dfs[1]




def main():
    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    # Data from Microstrategy is in S3
    # 2020 Bond Expenses Obligated.csv
    bond_data_2020 = get_data(client, "expenses_obligated_2020_bond_raw")
    bond_data_2020, py_bond_data_2020 = expenses_obligated(bond_data_2020)

    all_bond_data =  get_data(client,"expenses_obligated_all_bonds_raw")
    app = get_data(client, "all_bonds_appropriations")

    all_bond_data = all_bond_expenses_obligated(all_bond_data, app)

    s3_resource = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS
    )

    df_to_s3(bond_data_2020, s3_resource, "curr_fyear_obligated_expenses", False)
    df_to_s3(py_bond_data_2020, s3_resource, "prev_fyear_obligated_expenses", False)
    df_to_s3(all_bond_data, s3_resource, "all_bonds_obligation_expenses", False)

    fy = determine_fy(client)

    py_summary, cy_summary = summary_table(bond_data_2020, fy, client)
    df_to_s3(cy_summary, s3_resource, "curr_year_table", True)
    df_to_s3(py_summary, s3_resource, "prev_year_table", True)


if __name__ == "__main__":
    main()

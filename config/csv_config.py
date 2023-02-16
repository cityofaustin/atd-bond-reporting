"""
Configuration for the data sources of CSVs
"""

from pandera import Column, DataFrameSchema, Check

## CSV Endpoints
# Microstrategy
# 2020 Bond Expenses Obligated.csv
BOND_2020_EXP = "https://atd-microstrategy-reports.s3.amazonaws.com/2020+Bond+Expenses+Obligated.csv"
# All bonds Expenses Obligated.csv
ALL_BONDS_EXP = "https://atd-microstrategy-reports.s3.amazonaws.com/All+bonds+Expenses+Obligated.csv"

# Google Docs:
# 2020 AIMS to Dashboard ID Lookup table
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
# All bonds spend plan
SPEND_PLAN_ALL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhT8BEVrEMi3ISFPiz8ujKqmIkBgX9kvEQCdTU3eneG46cCr1-Cf1KJ5wovsej6gNPYx9UBEGN4VKi/pub?gid=1092895732&single=true&output=csv"
# All bonds Baseline spend
BASELINE_SPEND_ALL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhT8BEVrEMi3ISFPiz8ujKqmIkBgX9kvEQCdTU3eneG46cCr1-Cf1KJ5wovsej6gNPYx9UBEGN4VKi/pub?gid=0&single=true&output=csv"
# 2020 Bond Program Name lookup Table
BOND_PROG_NAMES = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSGZb6KRHwiSgxxsIrmxzDtEFR8Dg1QFfQ65dybwBv_EvZRCh3Fi1YqOP3vYI1uOe8M5ZZVFVuvUkZ-/pub?gid=215255456&single=true&output=csv"

CSVS = [
    {
        "url": BOND_2020_EXP,  # URL of the CSV file
        "table": "expenses_obligated_2020_bond_raw",  # Name of the table in the DB
        "date_field": True,  # True if this CSV has a "date" field
        "field_maps": {  # Mapping between CSV column names and those expected by the table
            "Fund": "fund",
            "Department": "department",
            "Date": "date",
            "Group": "group",
            "Fiscal Year": "fiscal_year",
            "Division": "division",
            "Obligated": "obligated",
            "Expenses": "expenses",
        },
        "schema": DataFrameSchema(  # Schema expected by the table
            {
                "fund": Column(str),
                "department": Column(int),
                "date": Column(str),
                "group": Column(str),
                "fiscal_year": Column(int, Check.greater_than(2000)),
                "division": Column(str),
                "obligated": Column(float),
                "expenses": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": ALL_BONDS_EXP,
        "table": "expenses_obligated_all_bonds_raw",
        "date_field": True,
        "field_maps": {
            "Fund@Code": "fund_code",
            "Fund@Long Name": "fund_long_name",
            "Division (As-Is)@Code": "division_code",
            "Division (As-Is)@Long Name": "division_long_name",
            "Department@Dept": "department",
            "Department@Long Name": "department_long_name",
            "Date": "date",
            "Group (As-Is)@Code": "group_code",
            "Group (As-Is)@Long Name": "group_long_name",
            "Obligated": "obligated",
            "Expenses": "expenses",
        },
        "schema": DataFrameSchema(
            {
                "fund_code": Column(str),
                "fund_long_name": Column(str),
                "date": Column(str),
                "division_code": Column(str),
                "division_long_name": Column(str),
                "department": Column(int),
                "department_long_name": Column(str),
                "group_code": Column(str),
                "group_long_name": Column(str),
                "obligated": Column(float),
                "expenses": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": AIMS_DASHBOARD,
        "table": "bond_2020_aims_to_dashboard",
        "date_field": False,
        "field_maps": {
            "AIMS Dept Prog Act": "aims_dept_prog_act",
            "Dashboard DeptFundProgAct": "dashboard_deptfundprogact",
        },
        "schema": DataFrameSchema(
            {
                "aims_dept_prog_act": Column(str),
                "dashboard_deptfundprogact": Column(str),
            },
            strict=True,
        ),
    },
    {
        "url": BASELINE_SPEND,
        "table": "bond_2020_baseline_spend",
        "date_field": True,
        "field_maps": {
            "Dashboard DeptFundProgAct": "dashboard_deptfundprogact",
            "Date": "date",
            "Amount": "amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "date": Column(str),
                "amount": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": CY_SPEND_PLAN,
        "table": "bond_2020_current_fy_spend_plan",
        "date_field": True,
        "field_maps": {
            "Dashboard DeptFundProgAct": "dashboard_deptfundprogact",
            "Date": "date",
            "Amount": "amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "date": Column(str),
                "amount": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": PY_SPEND_PLAN,
        "table": "bond_2020_previous_fy_spend_plan",
        "date_field": True,
        "field_maps": {
            "Dashboard DeptFundProgAct": "dashboard_deptfundprogact",
            "Date": "date",
            "Amount": "amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "date": Column(str),
                "amount": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": APPRO_DATA,
        "table": "all_bonds_appropriations",
        "date_field": True,
        "field_maps": {
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
            "Date": "date",
            "Appropriation": "amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "date": Column(str),
                "amount": Column(int),
            },
            strict=True,
        ),
    },
    {
        "url": DEPTFUNDPROGACT,
        "table": "all_bonds_program_names",
        "date_field": False,
        "field_maps": {
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
            "Dashboard Dept Name": "department_name",
            "Dashboard Bond Year": "bond_year",
            "Dashboard Program Name": "program_name",
            "Dashboard Sub Program Name": "sub_program_name",
            "Program Sort": "program_sort",
            "Sub Program Sort": "sub_program_sort",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "department_name": Column(str),
                "bond_year": Column(int),
                "program_name": Column(str),
                "sub_program_name": Column(str),
                "program_sort": Column(int),
                "sub_program_sort": Column(int),
            },
            strict=True,
        ),
    },
    {
        "url": AIMS_ALL_BONDS_DASHBOARD,
        "table": "all_bonds_aims_to_dashboard",
        "date_field": False,
        "field_maps": {
            "AIMS Dept Prog Act": "aims_dept_prog_act",
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
        },
        "schema": DataFrameSchema(
            {
                "aims_dept_prog_act": Column(str),
                "dashboard_deptfundprogact": Column(str),
            },
            strict=True,
        ),
    },
    {
        "url": SPEND_PLAN_ALL,
        "table": "all_bonds_spend_plan",
        "date_field": False,
        "field_maps": {
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
            "Fiscal Year": "fiscal_year",
            "SpndPln-Bdgt":"amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "fiscal_year": Column(int),
                "amount": Column(int),
            },
            strict=True,
        ),
    },
    {
        "url": BASELINE_SPEND_ALL,
        "table": "all_bonds_baseline_spend",
        "date_field": True,
        "field_maps": {
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
            "Date": "date",
            "Amount": "amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "date": Column(str),
                "amount": Column(int),
            },
            strict=True,
        ),
    },
    {
        "url": BOND_PROG_NAMES,
        "table": "bond_2020_program_names",
        "date_field": False,
        "field_maps": {
            "Dashboard DeptFundProgAct": "dashboard_deptfundprogact",
            "Dashboard Dept Name": "department_name",
            "Dashboard Bond Year": "bond_year",
            "Dashboard Program Name": "program_name",
            "Dashboard Sub Program Name": "sub_program_name",
            "Bond Funding": "funding_amount",
            "Program Sort": "program_sort",
            "Sub Program Sort": "sub_program_sort",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "department_name": Column(str),
                "bond_year": Column(int),
                "program_name": Column(str),
                "sub_program_name": Column(str),
                "funding_amount": Column(int),
                "program_sort": Column(int),
                "sub_program_sort": Column(int),
            },
            strict=True,
        ),
    },
]

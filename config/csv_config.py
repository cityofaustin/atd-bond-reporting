"""
Configuration for the data sources of CSVs
"""

from pandera import Column, DataFrameSchema, Check

# Microstrategy file names in S3
BOND_2020_EXP = "2020 Bond Expenses Obligated.csv"
ALL_BONDS_EXP = "All bonds Expenses Obligated.csv"
FDUS_2020 = "2020 FDUs with Subproject and Appropriation.csv"
SUBPROJECT_APPRO = "Subprojects with total Appropriation.csv"
SUBPROJECT_BUDGET = "Open Subprojects with Budget Estimate.csv"
FDU_EXPENSES = "FDU Expenses by Quarter.csv"
FDU_METADATA = "2020 Division Group and Unit.csv"

## CSV Endpoints
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
# Quarterly Spend Plan
QUARTERLY_SPEND_PLAN = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSQCnILsJPgR7WqJgsOHobhQhVR5iZX4tzBeQi7AwWaBsflrpV5P1Lg0azwsiqqSVwLzRDfxe-gEgxP/pub?gid=0&single=true&output=csv"

CSVS = [
    {
        "url": BOND_2020_EXP,  # URL of the CSV file
        "table": "expenses_obligated_2020_bond_raw",  # Name of the table in the DB
        "date_field": True,  # True if this CSV has a "date" field
        "boto3": True,  # True if we should use boto3 to read this file from S3
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
        "boto3": True,
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
        "url": FDUS_2020,
        "table": "fdus_2020_bond",
        "date_field": False,
        "boto3": True,
        "field_maps": {
            "Subproject@Number": "subproject_number",
            "Subproject@Name": "subproject_name",
            "Subproject@Status": "subproject_status",
            "Unit@Unit Code": "unit_code",
            "Unit@Long Name": "unit_name",
            "Appropriated": "appropriated",
        },
        "schema": DataFrameSchema(
            {
                "subproject_number": Column(float),
                "subproject_name": Column(str),
                "subproject_status": Column(str),
                "unit_code": Column(str),
                "unit_name": Column(str),
                "appropriated": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": SUBPROJECT_APPRO,
        "table": "subprojects_with_appropriations",
        "date_field": False,
        "boto3": True,
        "field_maps": {
            "Subproject@Number": "subproject_number",
            "Subproject@Name": "subproject_name",
            "Budget": "subproject_appropriation",
        },
        "schema": DataFrameSchema(
            {
                "subproject_number": Column(float),
                "subproject_name": Column(str),
                "subproject_appropriation": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": SUBPROJECT_BUDGET,
        "table": "subprojects_with_budget",
        "date_field": False,
        "boto3": True,
        "field_maps": {
            "Subproject@Number": "subproject_number",
            "Subproject@Name": "subproject_name",
            "Subproject@Status": "subproject_status",
            "Current Budget Estimate Amount": "project_estimate",
        },
        "schema": DataFrameSchema(
            {
                "subproject_number": Column(float),
                "subproject_name": Column(str),
                "subproject_status": Column(str),
                "project_estimate": Column(int),
            },
            strict=True,
        ),
    },
    {
        "url": FDU_EXPENSES,
        "table": "fdu_expenses_quarterly",
        "date_field": False,
        "boto3": True,
        "field_maps": {
            "Fund": "fund",
            "Department": "department",
            "Fiscal Quarter@Name": "fiscal_quarter",
            "Fiscal Quarter@Fiscal Year": "fiscal_year",
            "Subproject@Number": "subproject_number",
            "Subproject@Name": "subproject_name",
            "Unit@Unit Code": "unit_code",
            "Unit@Long Name": "unit_long_name",
            "Fiscal Month-Fiscal Year": "month-year",
            "Expenses": "expenses",
        },
        "schema": DataFrameSchema(
            {
                "fund": Column(str),
                "department": Column(int),
                "fiscal_quarter": Column(str),
                "fiscal_year": Column(int),
                "subproject_number": Column(float),
                "subproject_name": Column(str),
                "unit_code": Column(str),
                "unit_long_name": Column(str),
                "month-year": Column(str),
                "expenses": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": FDU_METADATA,
        "table": "fdu_metadata_quarterly",
        "date_field": False,
        "boto3": True,
        "field_maps": {
            "Subproject@Number": "subproject_number",
            "Subproject@Name": "subproject_name",
            "Group (As-Is)@Code": "subprogram_code",
            "Group (As-Is)@Long Name": "subprogram_long_name",
            "Division (As-Is)@Code": "program_code",
            "Division (As-Is)@Long Name": "program_long_name",
            "Unit@Unit Code": "unit_code",
            "Unit@Long Name": "unit_long_name",
            "Appropriated": "appropriated",
        },
        "schema": DataFrameSchema(
            {
                "subproject_number": Column(float),
                "subproject_name": Column(str),
                "subprogram_code": Column(str),
                "subprogram_long_name": Column(str),
                "program_code": Column(str),
                "program_long_name": Column(str),
                "unit_code": Column(str),
                "unit_long_name": Column(str),
                "appropriated": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": AIMS_DASHBOARD,
        "table": "bond_2020_aims_to_dashboard",
        "date_field": False,
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
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
        "boto3": False,
        "field_maps": {
            "Dashboard FundDeptProgAct": "dashboard_deptfundprogact",
            "Fiscal Year": "fiscal_year",
            "SpndPln-Bdgt":"amount",
        },
        "schema": DataFrameSchema(
            {
                "dashboard_deptfundprogact": Column(str),
                "fiscal_year": Column(int),
                "amount": Column(float),
            },
            strict=True,
        ),
    },
    {
        "url": BASELINE_SPEND_ALL,
        "table": "all_bonds_baseline_spend",
        "date_field": True,
        "boto3": False,
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
        "boto3": False,
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
    {
        "url": QUARTERLY_SPEND_PLAN,
        "table": "quarterly_spend_plan",
        "date_field": False,
        "boto3": False,
        "field_maps": {
            "Unit": "unit_code",
            "Unit Name": "unit_name",
            "Fiscal Year": "fiscal_year",
            "Quarter": "quarter",
            "Month": "month",
            "Spend Plan": "spend_plan",
        },
        "schema": DataFrameSchema(
            {
                "unit_code": Column(str),
                "unit_name": Column(str),
                "fiscal_year": Column(int),
                "quarter": Column(int),
                "month": Column(str),
                "spend_plan": Column(float),
            },
            strict=True,
        ),
    },
]

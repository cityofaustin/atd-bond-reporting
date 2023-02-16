# Maps fields between the column names from Microstrategy to
# the column names in the postgres table.

FIELD_MAPS = {
    "expenses_obligated_2020_bond_raw": {
        "Fund": "fund",
        "Department": "department",
        "Date": "date",
        "Group": "group",
        "Fiscal Year": "fiscal_year",
        "Division": "division",
        "Obligated": "obligated",
        "Expenses": "expenses",
    },
    "expenses_obligated_all_bonds_raw": {
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
}

import pandera as pa
from pandera import Column, DataFrameSchema, Check

SCHEMAS = {
    "expenses_obligated_2020_bond_raw": DataFrameSchema(
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
        strict=True
    ),
    "expenses_obligated_all_bonds_raw": DataFrameSchema(
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
        strict=True
    )
}

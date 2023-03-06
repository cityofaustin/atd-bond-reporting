CREATE TABLE api.expenses_obligated_2020_bond_raw (
  "fund" text,
  "department" int,
  "date" date,
  "group" text,
  "fiscal_year" int,
  "division" text,
  "obligated" numeric,
  "expenses" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.expenses_obligated_all_bonds_raw (
  "fund_code" text,
  "fund_long_name" text,
  "division_code" text,
  "division_long_name" text,
  "department" int,
  "department_long_name" text,
  "date" date,
  "group_code" text,
  "group_long_name" text,
  "obligated" numeric,
  "expenses" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.bond_2020_baseline_spend (
  "dashboard_deptfundprogact" text,
  "date" date,
  "amount" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.bond_2020_current_fy_spend_plan (
  "dashboard_deptfundprogact" text,
  "date" date,
  "amount" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.bond_2020_previous_fy_spend_plan (
  "dashboard_deptfundprogact" text,
  "date" date,
  "amount" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.bond_2020_program_names (
  "dashboard_deptfundprogact" text,
  "department_name" text,
  "bond_year" int,
  "program_name" text,
  "sub_program_name" text,
  "funding_amount" numeric,
  "program_sort" int,
  "sub_program_sort" int,
  "updated_at" timestamp
);


CREATE TABLE api.bond_2020_aims_to_dashboard (
  "aims_dept_prog_act" text,
  "dashboard_deptfundprogact" text,
  "updated_at" timestamp
);

CREATE TABLE api.all_bonds_baseline_spend (
  "dashboard_deptfundprogact" text,
  "date" date,
  "amount" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.all_bonds_spend_plan (
  "dashboard_deptfundprogact" text,
  "fiscal_year" int,
  "amount" numeric,
  "updated_at" timestamp
);

CREATE TABLE api.all_bonds_appropriations (
  "dashboard_deptfundprogact" text,
  "date" date,
  "amount" numeric,
  "updated_at" timestamp
);


CREATE TABLE api.all_bonds_program_names (
  "dashboard_deptfundprogact" text,
  "department_name" text,
  "bond_year" int,
  "program_name" text,
  "sub_program_name" text,
  "program_sort" int,
  "sub_program_sort" int,
  "updated_at" timestamp
);


CREATE TABLE api.all_bonds_aims_to_dashboard (
  "aims_dept_prog_act" text,
  "dashboard_deptfundprogact" text,
  "updated_at" timestamp
);



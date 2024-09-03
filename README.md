# ATD Bond Reporting

ETL that is the backend of the ATD/PWD Bond Spending Dashboard in Power BI. 

![Data flow diagram](images/data-flow-diagram.png?raw=true)

***

## microstrategy_to_s3.py

This script runs and then downloads a report from Microstrategy and then uploads it as a csv file in a S3 bucket. 

### Config

The `REPORTS` dict (`report_name`:`report_id`) at the top of this file stores what reports can be extracted. 

### Power BI

The purpose of this script is to make it easier to access Microstrategy reports in external applications like Power BI. The URL for all objects in the S3 bucket is default to publicly accessible. bond_data.py and bond_calculations.py build off of this. 

### Configuration and running

Run this script with the `-r` argument to provide the key of `REPORTS` configuration dict at the top of `microstrategy_to_s3.py` you want to run. This will create a file in the S3 bucket with the same name. 

```
REPORTS = {
    "a report name you pick": "abc123", # Microstrategy Report ID
}

```

To find a report's ID, go to the report page in Microstrategy then, go to Tools > "Report Details" Page or "Document Details" Page. Then, click the "Show Advanced Details" button at the bottom.

```
$ python microstrategy_to_s3.py -r "name of your report"
```

Example:

```
$ python microstrategy_to_s3.py -r "2020 Bond Expenses Obligated"
```

***

## bond_data.py

This script downloads data from various csv files and stores them in a postgres database. Every time this script is run, the old data is replaced. 

### Config

The config for this script is stored in config/csv_config.py. Each csv must have: 

- `url`: the URL the CSV lives at
- `table`: the name of the corresponding table in postgres
- `date_field`: boolean if there is a date column in this table (it also must be named `date`)
- `field_maps`: a dict of field mappings between the CSV's columns and the postgres columns.
- `schema`: a pandera schema that verifies that the CSV provided will be accepted by postgres

***

## bond_calculations.py

This script post-processes the bond spending data stored in the postgres DB, this data is then stored in Socrata datasets so Power BI can access them.

### Output tables

These three expenses tables show the individual expenses and obligations per-day and per-aims_dept_prog_act (similar to FDUs but is our group identifier in this case), and a `sum_expenses` and `sum_obligated` which are the cumulative sums for each group. The 2020 bond data is only for the 2020 bond, All bonds includes the 2016 and 2018 bonds as well.

- 2020 Bond Dashboard: Current Fiscal Year Expenses
- 2020 Bond Dashboard: Previous Fiscal Year Expenses
- Bond Dashboard: All Bonds Expenses

The summary tables group the above data by month (`table_col`) for the selected fiscal year and then groups all other months as fiscal years. Ex: for fiscal year 2022, January 2022 would be Jan-22 but for fiscal year 2023 it would be grouped into FY-22. 

- 2020 Bond Dashboard: Current Fiscal Year Summary Table
- 2020 Bond Dashboard: Previous Fiscal Year Summary Table

***

## Deployment

The provided Dockerfile will package this repo for deployment as an ETL in [airflow](https://github.com/cityofaustin/atd-airflow). This image is pushed to our [dockerhub repo](https://hub.docker.com/r/atddocker/atd-bond-reporting). Airflow uses this Docker container and a set of commands to orchestrate this ETL.

This repo has CI that will re-build and push to dockerhub the `production` and `latest` tags when a PR has been merged into `main`.

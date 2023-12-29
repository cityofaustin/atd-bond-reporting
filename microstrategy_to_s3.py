# Standard Library imports
import datetime
from io import StringIO
import os
import argparse

# Related third party imports
import boto3
from mstrio.connection import Connection
from mstrio.report import Report
import pandas as pd

BASE_URL = os.getenv("BASE_URL")
MSTRO_USERNAME = os.getenv("MSTRO_USERNAME")
MSTRO_PASSWORD = os.getenv("MSTRO_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")
AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET = os.getenv("BUCKET")

# Dict of: Report name: Report ID
REPORTS = {
    "2020 Bond Expenses Obligated": "39ED219649483E62CE430085EB2F48DE",
    "All bonds Expenses Obligated": "6B0DE57644C7C9912AAAE48392873233",
    "2020 FDUs with Subproject and Appropriation": "97DE7BEFEE4C93C18FE27199BBBA00BF",
    "Subprojects with total Appropriation": "01EF933E6D4FA79501CF10AF39DD8163",
    "Open Subprojects with Budget Estimate": "E2AC0ACEF944222D0039DA9FF07BA05C",
    "FDU Expenses by Quarter": "39EAC86D2F43664777ED6C9DBA27B43B",
    "2020 Division Group and Unit": "221FEFD57C4B215CCB6BE2BD8DEE8CA9",
}
# To find report ID, go to the report in Microstrategy then:
## Go to Tools > Report Details Page or Document Details Page.
## Click Show Advanced Details button at the bottom
# To view any of these reports in microstrategy (replace report_id):
# https://coa-prod.cloud.microstrategy.com:443/MicroStrategy/servlet/mstrWeb?&src=4001&evt=4001&reportViewMode=1&reportID=report_id


# Returns a connection object for interacting with the microstrategy API
def connect_to_mstro():
    conn = Connection(
        base_url=BASE_URL,
        username=MSTRO_USERNAME,
        password=MSTRO_PASSWORD,
        project_id=PROJECT_ID,
        login_mode=1,
    )
    return conn


# Returns a s3 object for interacting with the boto3 AWS S3 API
def connect_to_AWS():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_ID,
        aws_secret_access_key=AWS_PASS,
    )
    s3_res = session.resource("s3")

    return s3_res


# Downloads a report from microstrategy with a given report_id
# returns it as a pandas dataframe
def download_report(report_id, conn):
    my_report = Report(connection=conn, report_id=report_id, parallel=False)
    return my_report.to_dataframe()


# Takes a pandas dataframe and formats it to be sent as a .csv in an S3 bucket
# Uses the report_name.csv as a file name
# report_name should be unique or it'll overwrite another report
def report_to_s3(df, report_name, s3):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_name = f"{report_name}.csv"
    s3.Object(BUCKET, file_name).put(Body=csv_buffer.getvalue())


def main(args):
    if args.report_name not in REPORTS:
        raise "Report name not in configured reports."

    report_id = REPORTS[args.report_name]

    # 1. Get microstrategy connection
    conn = connect_to_mstro()

    # 2. Connect using boto3 to our S3
    s3 = connect_to_AWS()

    # 3. Download report to df
    df = download_report(report_id, conn)

    # 4. Send file to S3 bucket
    report_to_s3(df, args.report_name, s3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r",
        "--report-name",
        type=str,
        help="str: Name of the Microstrategy Report to download, as defined in the config.",
        required=True,
    )

    args = parser.parse_args()

    main(args)

from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator


@dag(schedule="@daily",
     start_date=datetime(2024, 1, 1),
     description="Download UK Premier League football fixtures data",
     catchup=False)
def fetch_fixturedownload_football_uk_premier_league():
    HttpToS3Operator(
        task_id="fetch",
        http_conn_id="http_fixturedownload_com",
        endpoint="/feed/json/epl-2024",
        s3_bucket="com-cloudership-prod-eu-north-1-airflow-testing",
        s3_key="2024/football/uk/premier_league-{{ ts }}.json",
        replace=True,
    )


fetch_fixturedownload_football_uk_premier_league()

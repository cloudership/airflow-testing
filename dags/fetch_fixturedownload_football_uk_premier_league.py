from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator


@dag(schedule="@daily",
     start_date=datetime(2024, 1, 1),
     description="Download UK Premier League football fixtures data",
     catchup=False)
def fetch_fixturedownload_football_uk_premier_league():
    # @task()
    # def fetch() -> List[Dict]:
    #     year = datetime.now(UTC).year
    #     import urllib.request
    #     url = f"https://fixturedownload.com/feed/json/epl-{year}"
    #     with urllib.request.urlopen(url) as file:
    #         logging.info(f"Fetching {url}")
    #         return json.load(file)

    # create_object = S3CreateObjectOperator(
    #     task_id="create_object",
    #     s3_bucket="com-cloudership-prod-eu-north-1-airflow-testing",
    #     s3_key="",
    #     data=DATA,
    #     replace=True,
    # )

    HttpToS3Operator(
        task_id="fetch",
        http_conn_id="http_fixturedownload_com",
        endpoint="/feed/json/epl-2024",
        s3_bucket="com-cloudership-prod-eu-north-1-airflow-testing",
        s3_key="2024/football/uk/premier_league.json",
        replace=True,
    )


fetch_fixturedownload_football_uk_premier_league()

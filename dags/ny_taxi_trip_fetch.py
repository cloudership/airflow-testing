import pandas
import requests
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from pendulum import datetime, DateTime

BUCKET_ROOT = ObjectStoragePath("s3://com-cloudership-prod-eu-north-1-airflow-testing/ny_taxi_trip_prediction/",
                                conn_id="aws_default")


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Fetch each month's NY taxi trip data",
     catchup=False)
def ny_yellow_taxi_trip_fetch():
    @task
    def fetch(logical_date: DateTime) -> ObjectStoragePath:
        # Sample: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
        endpoint = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{logical_date.year}-{logical_date.month:02}.parquet"
        path = BUCKET_ROOT / f"{logical_date.year}/{logical_date.month:02}/yellow.parquet"
        with requests.get(endpoint) as response:
            response.raise_for_status()
            with path.open("wb") as file:
                file.write(response.content)
        return path

    @task
    def check(path: ObjectStoragePath):
        with path.open("rb") as file:
            df = pandas.read_parquet(file)
        print(df.head())

    check(fetch())


ny_yellow_taxi_trip_fetch()

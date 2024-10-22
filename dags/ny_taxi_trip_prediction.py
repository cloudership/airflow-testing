import pandas
import requests
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from pendulum import datetime, DateTime

BUCKET_ROOT = ObjectStoragePath("s3://com-cloudership-prod-eu-north-1-airflow-testing/ny_taxi_trip_prediction/yellow/",
                                conn_id="aws_default")


@dag(schedule="@monthly",
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Fetch this month's NY yellow taxi trip data",
     catchup=False)
def ny_yellow_taxi_trip_prediction_pipeline():
    @task
    def fetch(logical_date: DateTime) -> ObjectStoragePath:
        # Sample: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
        endpoint = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{logical_date.year}-{logical_date.month:02}.parquet"
        path = BUCKET_ROOT / f"{logical_date.year}/{logical_date.month:02}/{logical_date.to_iso8601_string()}.parquet"
        with requests.get(endpoint) as response:
            response.raise_for_status()
            with path.open("wb") as file:
                file.write(response.content)
        return path

    @task
    def transform(path: ObjectStoragePath):
        with path.open("rb") as file:
            df = pandas.read_parquet(file)
        print(df.head())

    transform(fetch())


ny_yellow_taxi_trip_prediction_pipeline()

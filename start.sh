#!/bin/bash

set -euo pipefail

export _PIP_ADDITIONAL_REQUIREMENTS='aiobotocore boto3 apache-airflow[amazon] apache-airflow-providers-amazon[s3fs] apache-airflow[crypto] requests'

docker compose up

#!/bin/bash

set -euo pipefail

export _PIP_ADDITIONAL_REQUIREMENTS='apache-airflow[amazon]'

docker compose up

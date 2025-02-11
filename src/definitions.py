from dagster import Definitions
from dagster import EnvVar
from dagster_embedded_elt.dlt import DagsterDltResource

from src.assets.ny_taxi import asset_green_taxi, asset_yellow_taxi
from src.jobs import job_dbt_taxi
from src.jobs import job_ny_taxi
from src.assets.ny_taxi_dbt import dbt_assets_models
from src.schedule import schedules
from src.assets.ny_taxi_dbt import dbt_resource

from src.resources.s3 import S3Resource


defs = Definitions(
    assets=[asset_green_taxi, asset_yellow_taxi, dbt_assets_models],
    jobs=[job_ny_taxi, job_dbt_taxi],
    schedules=schedules,
    resources={
        "dlt": DagsterDltResource(),
        "filesystem": S3Resource(
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=EnvVar("ENDPOINT_URL"),
            bucket_url=EnvVar("BUCKET_URL"),
        ),
        "dbt": dbt_resource,
    },
)

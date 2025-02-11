from dagster import define_asset_job
from src.assets.ny_taxi import asset_green_taxi
from src.assets.ny_taxi import asset_yellow_taxi
from src.assets.ny_taxi_dbt import dbt_assets_models

job_ny_taxi = define_asset_job(
    "job_ny_taxi",
    selection=[asset_green_taxi, asset_yellow_taxi],
    run_tags={"asset_type": "ny_taxi"},
)


job_dbt_taxi = define_asset_job(
    "job_dbt_taxi",
    selection=[dbt_assets_models],
)
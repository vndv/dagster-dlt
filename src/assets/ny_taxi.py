import typing as t
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import MonthlyPartitionsDefinition
from dagster._annotations import public
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_embedded_elt.dlt import DagsterDltTranslator
from dagster_embedded_elt.dlt import dlt_assets
from dlt import destinations
from dlt import pipeline
from datetime import datetime
from dlt.extract.resource import DltResource

from src.resources.s3 import S3Resource
from src.sources.ny_taxi import ny_taxi_source


GROUP_NAME = DESTINATION_SCHEMA_NAME = "ny_taxi"
PARTITIONS_DEF = MonthlyPartitionsDefinition(
    start_date="2024-01-01", timezone="Europe/Moscow"
)


class TaxiDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        return AssetKey(["dlt", resource.source_name, resource.name])

    @public
    def get_tags(self, resource: DltResource) -> t.Mapping[str, str]:
        """A method that takes in a dlt resource and returns the Dagster tags of the structure."""
        return {"asset_type": resource.source_name}


def create_green_source():
    return ny_taxi_source(taxi_type="green", year="2024", month="1")


def create_yellow_source():
    return ny_taxi_source(taxi_type="yellow", year="2024", month="1")


@dlt_assets(
    group_name=GROUP_NAME,
    dlt_source=create_green_source(),
    dlt_pipeline=pipeline(
        destination="filesystem",
    ),
    partitions_def=PARTITIONS_DEF,
    dagster_dlt_translator=TaxiDagsterDltTranslator(),
    op_tags={"asset_type": "ny_taxi"},
)
def asset_green_taxi(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    filesystem: S3Resource,
):
    partition_key = context.partition_key
    date_obj = datetime.strptime(partition_key, "%Y-%m-%d")

    source = ny_taxi_source(taxi_type="green", year=date_obj.year, month=date_obj.month)

    dlt_pipeline = pipeline(
        pipeline_name="green_taxi",
        destination=destinations.filesystem(
            credentials={
                "aws_access_key_id": filesystem.aws_access_key_id,
                "aws_secret_access_key": filesystem.aws_secret_access_key,
                "endpoint_url": filesystem.endpoint_url,
                "bucket_url": filesystem.bucket_url,
            },
        ),
        progress="log",
        dataset_name=DESTINATION_SCHEMA_NAME,
    )

    yield from dlt.run(
        context=context,
        table_name="green_taxi",
        dlt_source=source,
        dlt_pipeline=dlt_pipeline,
    )


@dlt_assets(
    group_name=GROUP_NAME,
    dlt_source=create_yellow_source(),
    dlt_pipeline=pipeline(
        destination="filesystem",
    ),
    partitions_def=PARTITIONS_DEF,
    dagster_dlt_translator=TaxiDagsterDltTranslator(),
    op_tags={"asset_type": "ny_taxi"},
)
def asset_yellow_taxi(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    filesystem: S3Resource,
):
    partition_key = context.partition_key
    date_obj = datetime.strptime(partition_key, "%Y-%m-%d")

    source = ny_taxi_source(
        taxi_type="yellow", year=date_obj.year, month=date_obj.month
    )

    dlt_pipeline = pipeline(
        pipeline_name="yellow_taxi",
        destination=destinations.filesystem(
            credentials={
                "aws_access_key_id": filesystem.aws_access_key_id,
                "aws_secret_access_key": filesystem.aws_secret_access_key,
                "endpoint_url": filesystem.endpoint_url,
                "bucket_url": filesystem.bucket_url,
            },
        ),
        progress="log",
        dataset_name=DESTINATION_SCHEMA_NAME,
    )

    yield from dlt.run(
        context=context,
        table_name="yellow_taxi",
        dlt_source=source,
        dlt_pipeline=dlt_pipeline,
    )

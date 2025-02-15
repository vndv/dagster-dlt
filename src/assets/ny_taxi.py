import typing as t
from datetime import datetime

from dagster import AssetExecutionContext, AssetKey, MonthlyPartitionsDefinition
from dagster._annotations import public
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    dlt_assets,
)
from dlt import destinations, pipeline
from dlt.extract.resource import DltResource

from src.resources.s3 import S3Resource
from src.sources.ny_taxi import ny_taxi_source

GROUP_NAME = DESTINATION_SCHEMA_NAME = "ny_taxi"
PARTITIONS_DEF = MonthlyPartitionsDefinition(
    start_date="2024-01-01", timezone="Europe/Moscow"
)

TaxiType = t.Literal["green", "yellow"]


class TaxiDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        return AssetKey(["dlt", resource.source_name, resource.name])

    @public
    def get_tags(self, resource: DltResource) -> t.Mapping[str, str]:
        return {"asset_type": resource.source_name}


def create_taxi_source(taxi_type: TaxiType) -> t.Any:
    return ny_taxi_source(taxi_type=taxi_type, year="2024", month="1")


def create_dlt_pipeline(
    pipeline_name: str, filesystem: S3Resource, dataset_name: str
) -> t.Any:
    return pipeline(
        pipeline_name=pipeline_name,
        destination=destinations.filesystem(
            credentials={
                "aws_access_key_id": filesystem.aws_access_key_id,
                "aws_secret_access_key": filesystem.aws_secret_access_key,
                "endpoint_url": filesystem.endpoint_url,
                "bucket_url": filesystem.bucket_url,
            },
        ),
        progress="log",
        dataset_name=dataset_name,
    )


def process_taxi_data(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    filesystem: S3Resource,
    taxi_type: TaxiType,
) -> t.Generator:
    partition_key = context.partition_key
    date_obj = datetime.strptime(partition_key, "%Y-%m-%d")

    source = ny_taxi_source(
        taxi_type=taxi_type, year=date_obj.year, month=date_obj.month
    )
    dlt_pipeline = create_dlt_pipeline(
        f"{taxi_type}_taxi", filesystem, DESTINATION_SCHEMA_NAME
    )

    yield from dlt.run(
        context=context,
        table_name=f"{taxi_type}_taxi",
        dlt_source=source,
        dlt_pipeline=dlt_pipeline,
    )


@dlt_assets(
    group_name=GROUP_NAME,
    dlt_source=create_taxi_source("green"),
    dlt_pipeline=pipeline(destination="filesystem"),
    partitions_def=PARTITIONS_DEF,
    dagster_dlt_translator=TaxiDagsterDltTranslator(),
    op_tags={"asset_type": "ny_taxi"},
)
def asset_green_taxi(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    filesystem: S3Resource,
) -> t.Generator:
    yield from process_taxi_data(context, dlt, filesystem, "green")


@dlt_assets(
    group_name=GROUP_NAME,
    dlt_source=create_taxi_source("yellow"),
    dlt_pipeline=pipeline(destination="filesystem"),
    partitions_def=PARTITIONS_DEF,
    dagster_dlt_translator=TaxiDagsterDltTranslator(),
    op_tags={"asset_type": "ny_taxi"},
)
def asset_yellow_taxi(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    filesystem: S3Resource,
) -> t.Generator:
    yield from process_taxi_data(context, dlt, filesystem, "yellow")

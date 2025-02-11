import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject
from pathlib import Path


# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent.parent.parent / "ny_taxi"
dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_assets_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

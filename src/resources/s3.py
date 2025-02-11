from dagster import ConfigurableResource


class S3Resource(ConfigurableResource):
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str
    bucket_url: str

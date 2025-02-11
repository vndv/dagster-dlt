import os

from dagster import get_dagster_logger
from pydantic_settings import BaseSettings


logger = get_dagster_logger()


class AppSettings(BaseSettings):
    ENV: str = os.getenv("ENV", "")


settings = AppSettings()

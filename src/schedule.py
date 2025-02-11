from dagster import build_schedule_from_partitioned_job

from src.jobs import job_ny_taxi


schedules = [
    build_schedule_from_partitioned_job(
        job=job_ny_taxi,
        hour_of_day=4,
        minute_of_hour=8,
    ),
]

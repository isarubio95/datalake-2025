from dagster import Definitions
from .ingest_silver_job import ingest_silver_job, s3_new_objects_sensor_silver
from .organize_parquet import organize_parquet, daily_organization
from .ingest_worker_original import load_data_to_spark, s3_sensor
from .clone_metastore import register_existing_warehouses_job

defs = Definitions(
    jobs=[ingest_silver_job, organize_parquet, load_data_to_spark, register_existing_warehouses_job],
    schedules=[daily_organization],
    sensors=[s3_new_objects_sensor_silver, s3_sensor],
)

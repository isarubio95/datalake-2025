import os
from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo
from typing import Iterable

import boto3
import pandas as pd
from dagster import (
    sensor, RunRequest, job, op, get_dagster_logger, OpExecutionContext, In, Failure
)
from dagster_dbt import DbtCliResource
from pyspark.sql import SparkSession

# ---------- Config ----------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET        = os.getenv("S3_BUCKET", "ngods")
INGEST_PREFIX = os.getenv("S3_PREFIX",  "ingest/")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/")
TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")

dbt = DbtCliResource(project_dir="/work/dbt", profiles_dir=DBT_PROFILES_DIR)


# ---------- Helpers ----------
def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

def _local_parts(ts_utc: datetime) -> dict:
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)
    return {
        "y":  ts_local.year,
        "m":  f"{ts_local.month:02d}",
        "d":  f"{ts_local.day:02d}",
        "hh": f"{ts_local.hour:02d}",
        "mm": f"{ts_local.minute:02d}",
        "ts": ts_local,
    }

def _list_objects(prefix: str) -> Iterable[dict]:
    s3 = make_s3()
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) if "Contents" in resp else []:
            if not obj["Key"].endswith("/"):
                yield obj
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


# ---------- Ops ----------
@op
def download_from_minio_silver(key: str) -> bytes:
    s3 = make_s3()
    buf = BytesIO()
    s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=buf)
    buf.seek(0)
    get_dagster_logger().info(f"Downloaded {key} ({buf.getbuffer().nbytes} bytes)")
    return buf.getvalue()

@op
def validate_is_excel_silver(payload: bytes, key: str) -> str:
    k = key.lower()
    if k.endswith(".xlsx"): return "xlsx"
    if k.endswith(".xls"):  return "xls"
    if k.endswith(".csv"):  return "csv"
    raise Failure(f"The file {key} is not an Excel file (.xlsx/.xls/.csv)")

@op
def parse_excel_to_dataframe_silver(payload: bytes, ext: str) -> pd.DataFrame:
    buf = BytesIO(payload)
    if ext == "csv":
        df = pd.read_csv(buf, sep=None, engine="python", encoding="utf-8-sig")
    elif ext == "xlsx":
        df = pd.read_excel(buf, engine="openpyxl")
    elif ext == "xls":
        df = pd.read_excel(buf, engine="xlrd")
    else:
        raise Failure(f"Unsupported extension: {ext}")
    df.columns = [str(c).strip() for c in df.columns]
    get_dagster_logger().info(f"Read {ext}: {len(df)} rows × {len(df.columns)} columns")
    return df

@op(ins={"key": In(default_value=None, description="Source S3 key (optional, taken from tags if not provided)")})
def upload_dataframe_as_parquet_silver(context: OpExecutionContext, df: pd.DataFrame, key: str | None) -> str:
    key = key or context.run.tags.get("s3_key")
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    parts = _local_parts(ts_utc)

    df = df.copy()
    df["year"], df["month"], df["day"], df["hour"], df["minute"] = (
        parts["y"], int(parts["m"]), int(parts["d"]), int(parts["hh"]), int(parts["mm"])
    )

    base = os.path.basename(key)
    stem = os.path.splitext(base)[0]
    uid = f"{int(datetime.now(timezone.utc).timestamp())%100000:05d}"
    out_key = (
        f"{SILVER_PREFIX.rstrip('/')}/"
        f"year={parts['y']}/month={parts['m']}/day={parts['d']}/hour={parts['hh']}/minute={parts['mm']}/"
        f"{stem}_{uid}.parquet"
    )

    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = make_s3()
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
    get_dagster_logger().info(
        f"Uploaded: s3://{BUCKET}/{out_key} (partition {parts['y']}-{parts['m']}-{parts['d']} {parts['hh']}:{parts['mm']} Europe/Madrid)"
    )
    return out_key

@op(ins={"parquet_key": In()})
def register_metadata_in_spark(context: OpExecutionContext, parquet_key: str):
    """
    Crea una sesión de Spark y registra el fichero Parquet en el Hive Metastore
    de forma robusta, evitando condiciones de carrera.
    """
    logger = get_dagster_logger()
    original_key = context.run.tags.get("s3_key") or context.run.tags.get("s_key") or "unknown_source_file"
    
    table_name_raw = os.path.splitext(os.path.basename(original_key))[0]
    table_name = "".join(c if c.isalnum() else '_' for c in table_name_raw).lower()
    
    db_name = "silver"
    full_table_name = f"iceberg.{db_name}.{table_name}"
    
    logger.info(f"Iniciando sesión de Spark para registrar la tabla '{full_table_name}'...")

    spark = (
        SparkSession.builder.appName(f"Metadata-Ingest-{table_name}")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", "thrift://metastore:9083")
        .config("spark.sql.warehouse.dir", f"s3a://{BUCKET}/warehouse")
        .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    logger.info("Sesión de Spark creada con éxito.")
    
    parquet_path = f"s3a://{BUCKET}/{parquet_key}"
    
    try:
        logger.info(f"Asegurando que la base de datos '{db_name}' existe en el catálogo 'iceberg'...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS iceberg.{db_name}")

        logger.info(f"Leyendo datos desde {parquet_path}...")
        df = spark.read.parquet(parquet_path)
        df.createOrReplaceTempView("new_data_view")

        # Obtenemos el esquema del DataFrame en formato DDL (ej: "col1 STRING, col2 INT")
        schema_ddl = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])
        
        # 1. Crear la tabla de forma atómica si no existe (sin datos).
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({schema_ddl}) USING iceberg"
        logger.info(f"Ejecutando DDL para asegurar la existencia de la tabla: {create_table_sql}")
        spark.sql(create_table_sql)

        # 2. Insertar los datos. Esta operación siempre es segura ahora
        insert_sql = f"INSERT INTO {full_table_name} SELECT * FROM new_data_view"
        logger.info(f"Ejecutando DML para insertar datos: {insert_sql}")
        spark.sql(insert_sql)

        count = spark.table(full_table_name).count()
        logger.info(f"Metadatos actualizados para la tabla '{full_table_name}'. Filas totales ahora: {count}")
        
    except Exception as e:
        logger.error(f"Error durante el registro de metadatos en Spark: {e}")
        raise Failure(f"Fallo al registrar en Spark para la tabla {table_name}")
    finally:
        logger.info("Cerrando sesión de Spark.")
        spark.stop()

@op
def run_dbt_build_silver() -> str:
    res = dbt.cli(["build"])
    res.wait()
    return "dbt build OK"


# ---------- Job ----------
@job
def ingest_silver_job():
    payload    = download_from_minio_silver()
    ext        = validate_is_excel_silver(payload)
    df         = parse_excel_to_dataframe_silver(payload, ext)
    parquet_key = upload_dataframe_as_parquet_silver(df)
    register_metadata_in_spark(parquet_key)
    run_dbt_build_silver()


# ---------- Sensor ----------
@sensor(job=ingest_silver_job, minimum_interval_seconds=3)
def s3_new_objects_sensor_silver(context):
    last_seen = context.cursor
    now_iso   = datetime.now(timezone.utc).isoformat()

    items = []
    for obj in _list_objects(INGEST_PREFIX):
        lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
        if last_seen is None or lm_iso > last_seen:
            items.append((obj["Key"], lm_iso))

    if not items:
        if last_seen is None:
            context.update_cursor(now_iso)
        return

    items.sort(key=lambda x: x[1])
    for key, lm_iso in items:
        yield RunRequest(
            run_key = f"{lm_iso}|{key}",
            run_config={
                "ops": {
                    "download_from_minio_silver": {"inputs": {"key": {"value": key}}},
                    "validate_is_excel_silver":   {"inputs": {"key": {"value": key}}},
                }
            },
            tags={"s3_key": key, "s3_last_modified": lm_iso},
        )

    context.update_cursor(items[-1][1])
# dagster_register_existing_warehouses.py
# Dagster 1.9.x | PySpark 3.3/3.4 con Iceberg + Hadoop-AWS en el runtime.

from urllib.parse import urlparse
from typing import Optional

import boto3
from dagster import op, job, Config, get_dagster_logger, DynamicOut, DynamicOutput, Definitions

# --------------------------
# Configs
# --------------------------

class ListExistingConfig(Config):
    dest_root: str                 # ej: s3://bbtwins-test/Data/portesa/uploads/
    require_metadata_dir: bool = True

class RegisterConfig(Config):
    db: str                        # ej: "portesa_uploads"
    metastore_uri: str             # ej: thrift://metastore-test:9083
    dest_root: str                 # igual que en ListExistingConfig (se pasa aquí también)
    aws_region: Optional[str] = None

# --------------------------
# Descubre tablas (subcarpetas)
# --------------------------

@op(out=DynamicOut(str))
def list_existing_tables_op(config: ListExistingConfig):
    """
    Lista subcarpetas de primer nivel bajo dest_root y emite el nombre de cada "tabla"
    que tenga /metadata/ (o _iceberg/metadata/). No toca datos.
    """
    log = get_dagster_logger()
    parsed = urlparse(config.dest_root)
    if parsed.scheme != "s3":
        raise ValueError("dest_root debe ser s3://")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    found = False

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            sub = cp["Prefix"]                    # p.ej. Data/portesa/uploads/<tabla>/
            name = sub.rstrip("/").split("/")[-1] # <tabla>

            meta_dir = f"{sub}metadata/"
            alt_meta_dir = f"{sub}_iceberg/metadata/"

            def _exists(dir_key: str) -> bool:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=dir_key, MaxKeys=1)
                return resp.get("KeyCount", 0) > 0

            has_meta = _exists(meta_dir) or _exists(alt_meta_dir)
            if config.require_metadata_dir and not has_meta:
                log.info(f"Omito {name}: no existe metadata/ ni _iceberg/metadata/ en {sub}")
                continue

            found = True
            log.info(f"Detectada tabla existente: {name}")
            yield DynamicOutput(name, mapping_key=name)

    if not found:
        log.warning("No se detectaron subcarpetas de tabla en el prefijo indicado.")

# --------------------------
# Registro de UNA tabla (sin mover datos)
# --------------------------

@op
def register_one_table_op(name: str, config: RegisterConfig) -> None:
    """
    Registra en Hive Metastore (catálogo 'ice_test') la tabla existente en S3,
    sin reescribir datos: usa register_table() apuntando al último v*.metadata.json.
    Si no está disponible, hace fallback a CREATE TABLE ... USING ICEBERG LOCATION ...
    """
    log = get_dagster_logger()
    dest_prefix = config.dest_root.rstrip("/") + f"/{name}/"

    parsed = urlparse(dest_prefix)
    if parsed.scheme != "s3":
        raise ValueError("dest_root debe ser s3://")
    bucket = parsed.netloc
    base = parsed.path.lstrip("/")

    s3 = boto3.client("s3")

    # Localiza carpeta de metadatos (metadata/ o _iceberg/metadata/)
    meta_dir = f"{base}metadata/"
    alt_meta_dir = f"{base}_iceberg/metadata/"

    def _dir_exists(key: str) -> bool:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    if _dir_exists(meta_dir):
        md_prefix = meta_dir
    elif _dir_exists(alt_meta_dir):
        md_prefix = alt_meta_dir
    else:
        raise ValueError(f"[{name}] No se encontró carpeta de metadatos en s3://{bucket}/{base}")

    # Busca el último v*.metadata.json
    paginator = s3.get_paginator("list_objects_v2")
    latest_meta = None
    latest_ts = None
    for page in paginator.paginate(Bucket=bucket, Prefix=md_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if fname.startswith("v") and fname.endswith(".metadata.json"):
                ts = obj["LastModified"]
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts
                    latest_meta = key

    if not latest_meta:
        raise ValueError(f"[{name}] No se encontró ningún v*.metadata.json en {md_prefix}")

    metadata_file_uri = f"s3://{bucket}/{latest_meta}"
    table_location = f"s3://{bucket}/{base}"

    # Spark para registrar en el catálogo Iceberg (Hive)
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(f"register-{name}")
        .config("spark.sql.catalog.ice_test", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice_test.type", "hive")
        .config("spark.sql.catalog.ice_test.uri", config.metastore_uri)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )
    if config.aws_region:
        builder = builder.config("spark.hadoop.aws.region", config.aws_region)

    spark = builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS ice_test.{config.db}")

    target_fqn = f"ice_test.{config.db}.{name}"

    # 1) Preferente: register_table() con metadata_file
    try:
        spark.sql(
            f"""
            CALL ice_test.system.register_table(
              namespace => '{config.db}',
              table => '{name}',
              metadata_file => '{metadata_file_uri}'
            )
            """
        )
        log.info(f"[{name}] Registrada por register_table().")
    except Exception as e:
        log.warning(f"[{name}] register_table() no disponible o falló ({e}). Fallback a CREATE TABLE LOCATION.")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {target_fqn}
            USING ICEBERG
            LOCATION '{table_location}'
            """
        )
        log.info(f"[{name}] Registrada por CREATE TABLE ... LOCATION.")

    # Comprobación rápida
    try:
        cnt = spark.table(target_fqn).count()
        log.info(f"[{name}] OK. Filas visibles: {cnt}")
    except Exception as e:
        log.warning(f"[{name}] Registrada, pero fallo al leer: {e}")

    spark.stop()

# --------------------------
# Job con config incrustada
# --------------------------

@job(
    name="register_existing_warehouses_job",
    config={
        "ops": {
            "list_existing_tables_op": {
                "config": {
                    "dest_root": "s3://bbtwins-test/Data/portesa/uploads/",
                    "require_metadata_dir": True
                }
            },
            "register_one_table_op": {
                "config": {
                    "db": "portesa_uploads",
                    "metastore_uri": "thrift://metastore:9083",
                    "dest_root": "s3://bbtwins-test/Data/portesa/uploads/",
                }
            }
        }
    }
)
def register_existing_warehouses_job():
    names = list_existing_tables_op()
    names.map(register_one_table_op)
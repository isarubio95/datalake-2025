from urllib.parse import urlparse
from typing import Optional
import os
import boto3
from dagster import op, job, Config, get_dagster_logger, DynamicOut, DynamicOutput, Definitions

# --------------------------
# Configs
# --------------------------
S3_REGION = os.getenv("AWS_REGION")

class ListExistingConfig(Config):
    dest_root: str                 # ej: s3://bbtwins-test/Data/portesa/uploads/
    require_metadata_dir: bool = True

class RegisterConfig(Config):
    catalog_name: str
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

    s3 = boto3.client("s3", region_name=S3_REGION)

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
            # 1. Reemplazar 'ñ' por 'n' y 'Ñ' por 'N' (como solicitaste)
            name_normalized = name.replace('ñ', 'n').replace('Ñ', 'N')
            
            # 2. Sanitizar el resto de caracteres no alfanuméricos (como '-') a '_'
            mapping_key = "".join(c if c.isalnum() else '_' for c in name_normalized)

            # Yield the *original* name as the value, but the *sanitized* name as the key
            yield DynamicOutput(name, mapping_key=mapping_key)

    if not found:
        log.warning("No se detectaron subcarpetas de tabla en el prefijo indicado.")

# --------------------------
# Registro de UNA tabla (sin mover datos)
# --------------------------

@op
def register_one_table_op(name: str, config: RegisterConfig) -> None:
    """
    Registra en Hive Metastore (catálogo 'iceberg') la tabla existente en S3,
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

    s3 = boto3.client("s3", region_name=S3_REGION)

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

    # Busca el último .metadata.json
    paginator = s3.get_paginator("list_objects_v2")
    latest_meta = None
    latest_ts = None
    for page in paginator.paginate(Bucket=bucket, Prefix=md_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if fname.endswith(".metadata.json"):
                ts = obj["LastModified"]
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts
                    latest_meta = key

    if not latest_meta:
        log.warning(f"[{name}] No se encontró ningún .metadata.json en {md_prefix}. Omitiendo esta tabla.")
        return # Salta esta tabla y continúa con la siguiente

    metadata_file_uri = f"s3a://{bucket}/{latest_meta}"
    table_location = f"s3a://{bucket}/{base}"

    # Spark para registrar en el catálogo Iceberg (Hive)
    from pyspark.sql import SparkSession
    
    # --- Usa el nombre del catálogo dinámicamente ---
    catalog_name = config.catalog_name
    
    builder = (
        SparkSession.builder
        .appName(f"register-{name}")
        .master("local[*]")
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "hive")
        .config(f"spark.sql.catalog.{catalog_name}.uri", config.metastore_uri)
        .config("spark.sql.warehouse.dir", f"s3a://{bucket}/Data/")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )
    if config.aws_region:
        builder = builder.config("spark.hadoop.aws.region", config.aws_region)

    spark = builder.getOrCreate()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{config.db}")

    target_fqn = f"{catalog_name}.{config.db}.{name}"

    try:
        log.info(f"[{name}] Intentando cargar (leer) la tabla desde: {table_location}")
        
        # 1. Carga la tabla existente desde S3
        df = spark.read.format("iceberg").load(table_location)
        
        log.info(f"[{name}] Tabla leída con éxito. Registrando en catálogo como {target_fqn}...")

        # 2. Guárdala/Regístrala en el catálogo (Hive Metastore)
        df.write.mode("overwrite").saveAsTable(target_fqn)

        log.info(f"[{name}] ¡Registrada con éxito en el catálogo!")

    except Exception as e:
        log.error(f"[{name}] Fallo al intentar cargar y registrar la tabla desde {table_location}. Error: {e}")
        # Si esto falla, lanzamos el error para ver por qué
        raise e

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
                    "catalog_name": "portesa",
                    "db": "uploads",
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
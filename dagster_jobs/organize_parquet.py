import os
import boto3
from dagster import (
    Definitions, job, op, get_dagster_logger, ScheduleDefinition, Config, Nothing
)

# ---------- Configuración ----------
BUCKET = os.getenv("S3_BUCKET", "bbtwins")
AWS_REGION = os.getenv("AWS_REGION")

# --- Configuración de la tabla ---
CATALOG_NAME = "portesa"
# catálogo.esquema.tabla
TABLE_NAME = "portesa.uploads.cacartesa_bombaaguagral" 

@op
def evolve_and_compact_table() -> None:
    """
    Evoluciona y compacta la tabla Iceberg.
    """
    import pyspark
    from pyspark.sql import SparkSession

    log = get_dagster_logger()
    
    # --- Configuración de la TABLA ÚNICA ---
    CATALOG_NAME = "portesa"
    SCHEMA_NAME = "uploads"
    BARE_TABLE_NAME = "cacartesa_bombaaguagral" # El nombre exacto de la tabla
    
    # Nombre de 2 partes (esquema.tabla)
    TABLE_NAME_2_PART = f"{SCHEMA_NAME}.{BARE_TABLE_NAME}"

    log.info(f"Iniciando Spark para evolucionar y compactar: {CATALOG_NAME}.{TABLE_NAME_2_PART}")

    spark = (
        SparkSession.builder.appName("iceberg-evolve-and-compact")
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # --- Configuración del Catálogo (Igual que antes) ---
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "thrift://metastore:9083")
        
        # --- ¡ESTA ES LA LÍNEA CLAVE DE LA SOLUCIÓN! ---
        # Le decimos a Spark que 'portesa' es el catálogo por defecto
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        # --- Fin de la línea clave ---
        
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.region", os.getenv("AWS_REGION"))
        .getOrCreate()
    )

    try:
        # --- PASO 1: Evolucionar la partición ---
        # Ahora usamos el nombre de 2 partes (schema.table)
        log.info(f"Aplicando especificación de partición 'days(Date)' a {TABLE_NAME_2_PART}...")
        spark.sql(f"""
            ALTER TABLE {TABLE_NAME_2_PART}
            ADD PARTITION FIELD days(Date)
        """)
        log.info("Evolución de partición completada.")

        # --- PASO 2: Reescribir y Compactar ---
        log.info(f"Iniciando reescritura/compactación de datos para {TABLE_NAME_2_PART}...")
        # La llamada al sistema SÍ necesita el nombre del catálogo
        spark.sql(f"""
            CALL {CATALOG_NAME}.system.rewrite_data_files(
                table => '{TABLE_NAME_2_PART}', 
                strategy => 'compact'
            )
        """)
        log.info("Reescritura y compactación completadas.")

    except Exception as e:
        log.error(f"Ha ocurrido un error durante la evolución/compactación: {e}")
        raise
    finally:
        spark.stop()

# ---------- Job & Schedule ----------
@job
def organize_parquet():
    """
    Job de mantenimiento que evoluciona la partición de la tabla
    y compacta los archivos para que coincidan.
    """
    evolve_and_compact_table()

daily_organization = ScheduleDefinition(
    job=organize_parquet,
    cron_schedule="0 3 * * *", # Todos los días a las 3 AM
    execution_timezone=os.getenv("TZ", "Europe/Madrid"),
)
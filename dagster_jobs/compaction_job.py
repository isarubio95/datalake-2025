import os
from pyspark.sql import SparkSession
from dagster import (
    job, 
    op, 
    get_dagster_logger, 
    Config, 
    Field, 
    Failure
)
from typing import List

# --- Configuración ---

class CompactionConfig(Config):
    schemas_to_compact: List[str] = Field(
        default_value=['silver', 'portesa.uploads'],
        description=(
            "Lista de schemas (bases de datos) en el catálogo 'iceberg' "
            "cuyas tablas se compactarán. Ej: ['silver', 'mi_otro_schema']"
        )
    )

# --- Helper de Spark (MODIFICADO PARA AWS S3) ---

def _get_spark_session(logger) -> SparkSession:
    """
    Crea y configura la sesión de Spark con soporte para Iceberg, S3 y Hive.
    --- MODIFICADO PARA AWS S3 (NO MINIO) ---
    """
    logger.info("Iniciando nueva sesión de Spark para compactación (modo AWS S3)...")
    
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise Failure("La variable de entorno S3_BUCKET no está definida.")

    builder = (
        SparkSession.builder.appName("Iceberg-Compaction-Job")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", "thrift://metastore:9083")
        # El warehouse.dir sigue apuntando al bucket S3
        .config("spark.sql.warehouse.dir", f"s3a://{bucket}/warehouse")
        .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        
        # --- Configuración para AWS S3 (reemplaza MinIO) ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # Usa la cadena de proveedores de credenciales de AWS por defecto.
        # Esto buscará credenciales en:
        # 1. Variables de entorno (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # 2. Perfil de instancia EC2 (IAM Role)
        # 3. Archivo ~/.aws/credentials
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        
        # Eliminamos las configuraciones específicas de MinIO:
        # - spark.hadoop.fs.s3a.endpoint
        # - spark.hadoop.fs.s3a.path.style.access
        # - spark.hadoop.fs.s3a.access.key (manejado por el provider)
        # - spark.hadoop.fs.s3a.secret.key (manejado por el provider)
    )
    
    spark = builder.getOrCreate()
    logger.info("Sesión de Spark (AWS S3) creada con éxito.")
    return spark

# --- Op ---

@op(config_schema=CompactionConfig)
def compact_all_tables_op(context):
    """
    Compacta ficheros pequeños en tablas Iceberg para los schemas configurados.
    
    Itera sobre cada schema, lista todas sus tablas y ejecuta
    'CALL iceberg.system.rewrite_data_files()' en cada una.
    """
    logger = get_dagster_logger()
    schemas = context.op_config["schemas_to_compact"]
    spark = None
    
    if not schemas:
        logger.warn("No se han configurado schemas para compactar. Saliendo.")
        return "No schemas configurados."

    logger.info(f"Iniciando trabajo de compactación para {len(schemas)} schema(s): {schemas}")
    
    summary = {
        "schemas_processed": 0,
        "tables_processed": 0,
        "tables_failed": 0,
        "files_rewritten": 0,
    }

    try:
        spark = _get_spark_session(logger)
        
        for schema_name in schemas:
            full_schema_name = f"iceberg.{schema_name}"
            logger.info(f"--- Procesando Schema: {full_schema_name} ---")
            
            try:
                # 1. Listar todas las tablas del schema
                tables_df = spark.sql(f"SHOW TABLES IN {full_schema_name}")
                table_names = [row.tableName for row in tables_df.collect()]
                
                if not table_names:
                    logger.warn(f"No se encontraron tablas en {full_schema_name}.")
                    continue
                
                logger.info(f"Encontradas {len(table_names)} tablas: {table_names}")
                summary["schemas_processed"] += 1

                # 2. Compactar cada tabla
                for table_name in table_names:
                    full_table_name = f"{full_schema_name}.{table_name}"
                    try:
                        logger.info(f"Compactando tabla: {full_table_name} ...")
                        
                        # Ejecuta la compactación (estrategia 'binpack' por defecto)
                        # Respeta las particiones existentes (año, mes, día...).
                        sql = f"CALL iceberg.system.rewrite_data_files(table => '{full_table_name}')"
                        
                        result_df = spark.sql(sql)
                        result = result_df.collect()[0].asDict()
                        
                        rewritten_count = result.get('rewritten_files_count', 0)
                        logger.info(
                            f"Compactación OK para {full_table_name}: "
                            f"{rewritten_count} archivos reescritos, "
                            f"{result.get('rewritten_data_files_size_bytes', 0)} bytes."
                        )
                        summary["tables_processed"] += 1
                        summary["files_rewritten"] += rewritten_count
                        
                    except Exception as e:
                        logger.error(f"Fallo al compactar {full_table_name}: {e}")
                        summary["tables_failed"] += 1
                        
            except Exception as e:
                logger.error(f"Fallo al procesar schema {full_schema_name} (quizás no existe?): {e}")

    except Exception as e:
        logger.error(f"Error crítico durante la sesión de Spark: {e}")
        raise Failure(f"Error en Spark: {e}")
    
    finally:
        if spark:
            spark.stop()
            logger.info("Sesión de Spark cerrada.")
            
    logger.info(f"Resumen de compactación: {summary}")
    
    if summary["tables_failed"] > 0:
        raise Failure(f"Fallaron {summary['tables_failed']} compactaciones de tablas.")

    return f"Compactación completada. {summary['files_rewritten']} archivos reescritos en {summary['tables_processed']} tablas."


# --- Job ---

@job
def compaction_job():
    """
    Job de Dagster para ejecutar la compactación de tablas Iceberg.
    Este job se puede ejecutar manualmente o con un schedule (p.ej. cada noche).
    """
    compact_all_tables_op()
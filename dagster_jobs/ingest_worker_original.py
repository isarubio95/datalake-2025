from pyspark.sql.types import IntegerType, DoubleType, BooleanType, StringType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from dagster import job, op, ScheduleDefinition, repository
from dagster import sensor, RunRequest, SkipReason, DefaultSensorStatus
from dagster import get_dagster_logger
from pathlib import Path
import boto3
import socket
import os
from datetime import date
import pandas as pd
import re
import time
from pyspark.sql.functions import current_timestamp


# Get Ip address
hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)

database = 'uploads'
bucket_name = 'bbtwins-test'

logger = get_dagster_logger()

dic_schemas = { 'gemelo_matanza.csv': StructType([
                                        StructField("LoteRecepID", IntegerType(), True),
                                        StructField("CarcassID", IntegerType(), True),
                                        StructField("Number", IntegerType(), True),
                                        StructField("Weight", DoubleType(), True),
                                        StructField("Fat", IntegerType(), True),
                                        StructField("NumRejectsHams", IntegerType(), True),
                                        StructField("NumRejectsShoulders", IntegerType(), True),
                                        StructField("CarcassType", IntegerType(), True),
                                        StructField("DescriptionCarcassType", StringType(), True),
                                        StructField("RejectsHamType", IntegerType(), True),
                                        StructField("RejectsHam", StringType(), True),
                                        StructField("RejectsShoulderType", IntegerType(), True),
                                        StructField("RejectsShoulder", StringType(), True),
                                        StructField("ConfiscationType", IntegerType(), True),
                                        StructField("ConfiscationDescription", StringType(), True),
                                        StructField("TypeClasifID", StringType(), True),
                                        StructField("DescripClasif", StringType(), True),
                                        StructField("Gender", StringType(), True),
                                        StructField("AptaIGP", IntegerType(), True),
                                        StructField("SUBTIPO_LOTE", StringType(), True),
                                        StructField("FechaRecep", TimestampType(), True),
                                        StructField("Fecha_subida", TimestampType(), True),
                                        StructField("IdRail", IntegerType(), True)
                                        ]),
                'gemelo_pedidos.csv': StructType([
                                        StructField("date", TimestampType(), True),
                                        StructField("descrip", StringType(), True),
                                        StructField("customer_id", StringType(), True),
                                        StructField("erp_code", StringType(), True),
                                        StructField("product", StringType(), True),
                                        StructField("kilos_order", StringType(), True),
                                        StructField("pieces_ordered", DoubleType(), True),
                                        StructField("boxes_ordered", IntegerType(), True),
                                        StructField("kilos_served", DoubleType(), True),
                                        StructField("pieces_served", DoubleType(), True),
                                        StructField("boxes_served", IntegerType(), True),
                                        StructField("kilos_dif", DoubleType(), True),
                                        StructField("diff_pieces", DoubleType(), True),
                                        StructField("diff_boxes", IntegerType(), True),
                                        StructField("fecha_entrega", StringType(), True),
                                        StructField("fecha_subida", TimestampType(), True)
                                    ]),
                'gemelo_partidas.csv': StructType([
                                            StructField("loterecepid", IntegerType(), True),
                                            StructField("codigo", StringType(), True),
                                            StructField("fecharecep", TimestampType(), True),
                                            StructField("descripgranja", StringType(), True),
                                            StructField("granjaorigen", StringType(), True),
                                            StructField("codigogranja", StringType(), True),
                                            StructField("codigodo", StringType(), True),
                                            StructField("animalesvivos", IntegerType(), True),
                                            StructField("animalesmuertos", IntegerType(), True),
                                            StructField("tipopartida", StringType(), True),
                                            StructField("numguiasanitaria", StringType(), True),
                                            StructField("pesototal", DoubleType(), True),
                                            StructField("numintegracion", IntegerType(), True),
                                            StructField("descripintegracion", StringType(), True),
                                            StructField("fechasacrif", TimestampType(), True),
                                            StructField("descripproveedor", StringType(), True),
                                            StructField("descrippropietario", StringType(), True),
                                            StructField("descripmatadero", StringType(), True),
                                            StructField("numpartidafact", IntegerType(), True),
                                            StructField("genetica", StringType(), True),
                                            StructField("numalbaran", StringType(), True),
                                            StructField("lotesalida", IntegerType(), True),
                                            StructField("fecha_subida", TimestampType(), True)
                                        ]),
                'in_propuesta_estimacion_semanal_weekly_plan.csv': StructType([
                                                                        StructField("slaughter", StringType(), True),
                                                                        StructField("cutting", StringType(), True),
                                                                        StructField("supplier", StringType(), True),
                                                                        StructField("destination", StringType(), True),
                                                                        StructField("do", IntegerType(), True),
                                                                        StructField("quantity", IntegerType(), True)
                                                                    ]),
            }

def spark_setup():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    return spark


s3 = boto3.client('s3', region_name=os.getenv("AWS_REGION"))


def add_table_columns(spark, df, table_name):
    # Adds column to table (avoid too many columns error)
    for col in df.columns:
        try:
            if col not in spark.table(table_name).columns:
                data_type = [item for item in df.dtypes if item[0]==col][0][1]
                query = f"ALTER TABLE {table_name} ADD COLUMNS (`{col}` {data_type})"
                spark.sql(query)
        except AnalysisException:
            pass


def write_data(name, warehouse, df, spark):
        logger.info(f"WRITE_DATA -------------------------------------> {name}")
        new_table = False

        # 1. Combine your variables into ONE valid database name
        db_name = f"{warehouse}_{database}" # This creates 'portesa_uploads'

        # 2. Creates database with the new valid name
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        # Writes spark dataframe to datalake table
        # 3. Use the new 2-part name
        table_name = f"{db_name}.`{name}`" # This becomes 'portesa_uploads.gemelo_pedidos'
        fields = []

        add_table_columns(spark, df, table_name)

        # Load existing data from the table
        try:
            existing_data = spark.table(table_name)
            new_data = df.exceptAll(existing_data) #compares dataframes and only adds new_data
            logger.info(f"PRIMER TRY -------------------------------------> {new_data}")
            
        except AnalysisException as e:
            logger.info(f"PRIMER EXCEPT -------------------------------------> {e}")
            print(e)
            print("New table")
            new_table = True
            new_data=df

        try:
            logger.info(f"SEGUNDO TRY -------------------------------------> {new_table}")

            new_data.write.mode("append").saveAsTable(table_name)
            logger.info(f"SEGUNDO TRY -------------------------------------> terminada la escritura")
            if new_table:
                #Changes table format-version to 2 so tables can be updated from Trino
                query = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '2');"
                spark.sql(query)
        except Exception as tm:
            print("***************** Unknown Exception please check ************************")
            print(tm)
            logger.info(f"SEGUNDO EXCEPT -------------------------------------> {tm}")
            if "too many data columns" in str(tm):
                print("Error: Write aborted, make sure the CSV header is not numeric.")
                logger.info(f"SEGUNDO EXCEPT -------------------------------------> {tmc}")


def create_table(warehouse,spark):
    #Creates raw_data table if not exits
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {warehouse}.{database}.raw_data (
    path string,
    filename string,
    extension string,
    date date);
    """)


def insert_to_table(warehouse,spark,path,file_name,extension=""):
    #Insert a record for any file copied to S3 raw_data folder
    create_table(warehouse,spark)
    now = date.today().strftime('%Y-%m-%d')
    spark.sql(f"""
    INSERT INTO {warehouse}.{database}.raw_data
    VALUES ('{path}', '{file_name}', '{extension}', date '{now}')
    """)


def convert_excel_to_csv(bucket_name, key, downloaded_file,spark,warehouse):
    #Creates a CSV for any excel sheet and upload it to S3
    s3.download_file(bucket_name, key, downloaded_file)
    excel_file = pd.ExcelFile(downloaded_file)
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name, keep_default_na=False, converters={'producer_id': lambda x: f'{x}', 'variety_id': lambda x: f'{x}'})
        if not df.columns.empty:
            for col in df.columns:
                df.loc[df[col]=="NA", col] = ""
                if col == "Date" or col == "datetime":
                    df[col] = pd.to_datetime(df[col], infer_datetime_format=True, dayfirst=True)

            try:
                df = df.drop(columns=["Unnamed: 3"])
            except KeyError:
                pass
            try:
                df = df.drop(columns=["Unnamed: 5"])
            except KeyError:
                pass

            sheet_name = re.sub(r'[-—–\s]', '_', sheet_name.lower())
        if 'gemelo_pedidos' in downloaded_file:
            csv_file_name = re.sub(r'[-—–\s]', '_', downloaded_file.split(".")[0].lower()) + ".csv"
        else:
            csv_file_name = re.sub(r'[-—–\s]', '_', downloaded_file.split(".")[0].lower()) + "_" + sheet_name + ".csv"
        df.to_csv(csv_file_name, index=False, encoding="utf-8", escapechar='\\')
        print("Create file  --> ",  csv_file_name)
        s3.upload_file(csv_file_name, bucket_name, key.rsplit('/', 1)[0] + "/" + csv_file_name.split("/")[-1])
        os.remove(csv_file_name)

    os.remove(downloaded_file)
    moved_file = move_file_to_raw_data(spark,warehouse,key)


@op
def process_data():
    spark = spark_setup()
    data_path = 'Upload_Data'
    result = s3.list_objects(Bucket=bucket_name, Prefix=data_path)
    files_list = result.get("Contents").copy()
    dfs = {}
    for content in files_list:
        key = content.get("Key")
        #if 'gemelo' in key or 'GEMELO' in key and False:
        #   logger.info("BLOQUEO DE GEMELO")
        #   continue
        if key.endswith("/") == False :
            logger.info(f"Processing file -----------------------------> {key}")
            file = key.split("/")[-1]
            warehouse = key.split("/")[1].lower()
            downloaded_file = f"/tmp/{file}"
            downloaded_file_ext = downloaded_file.split(".")[-1]
            
            if downloaded_file_ext == "csv":
                download_dir = os.path.dirname(downloaded_file)
                os.makedirs(download_dir, exist_ok=True)
                s3.download_file(bucket_name, key, downloaded_file)
                #converts csv to spark dataframe with encoding for any characters
                # it looks for special files, and makes its schema redone
                if file in dic_schemas:
                    logger.info('INTERCEPTADO')
                    df1 = spark.read.options(header=True, encoding='utf-8').schema(dic_schemas[file]).csv(downloaded_file)
                else:
                    df1 = spark.read.options(inferSchema=True,header=True,encoding='utf-8').csv(downloaded_file)

                # reads schema to find columns that contains "_id" to change it type to String
                if df1.count() > 0: #check if there is any data
                    string_columns = []
                    for col in df1.columns:
                        if "_" in col:
                            try:
                                splited_col = col.split("_")
                                if "id" in splited_col:
                                    string_columns.append(df1.columns.index(col))
                            except Exception as e:
                                print(e)
                    schema1=df1.schema
                    for col_index in string_columns:
                        schema1.fields[col_index].dataType=StringType()
                    df = spark.read.schema(schema1).csv(downloaded_file)

                    # Skip the first row (header) while reading the CSV
                    df = df1.limit(df1.count())

                    file_name = key.rsplit("/", 1)[1].rsplit(".", 1)[0]
       
                    write_data(file_name, warehouse, df, spark)

                move_file_to_raw_data(spark,warehouse,key)
                os.remove(downloaded_file)
            else:
                return
                move_file_to_raw_data(spark,warehouse,key)


def move_file_to_raw_data(spark, warehouse,key):
    print("Moves file ------> ", key)
    new_path = 'Data/'
    now = date.today()
    file = key.split("/")[-1]
    save_path = new_path+warehouse+"Files/"+str(now.year)+"/"+str(now.month)+"/"+str(now.day)+"/"+ file
    file_name = key.split("/")[-1].split(".")[0]
    file_extension = file.split(".")[-1]
    s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": key}, Key=save_path)
    s3.delete_object(Bucket=bucket_name, Key=key)
    insert_to_table(warehouse,spark,bucket_name+"/"+save_path,file_name,file_extension)

@job
def load_data_to_spark():
    process_data()


@sensor(job=load_data_to_spark, minimum_interval_seconds=1800, default_status=DefaultSensorStatus.RUNNING,)
def s3_sensor(context):
    # Consulta S3 para ver archivos en el folder 'Upload_Data'
    result = s3.list_objects(Bucket=bucket_name, Prefix='Upload_Data')
    
    # Si no se encontró el campo "Contents", significa que no hay archivos
    if "Contents" not in result:
        context.log.info("No se encontraron archivos en S3.")
        yield SkipReason("No files found")
        return

    archivos = result["Contents"]

    # Aquí podrías agregar lógica para determinar si los archivos son nuevos.
    # Por simplicidad, se dispara el job si se encuentra al menos un archivo.
    if archivos:
        # Se genera un run_key; idealmente debería ser único o basado en la última vez que se disparó
        run_key = f"{len(archivos)}-{int(time.time())}"
        context.log.info(f"Se encontraron {len(archivos)} archivos. Disparando el job con run_key: {run_key}")
        yield RunRequest(run_key=run_key, run_config={})
    else:
        yield SkipReason("No new files")


@repository
def workspace():
    return [
        load_data_to_spark,
    	#do_it_all_schedule
        s3_sensor
    ]

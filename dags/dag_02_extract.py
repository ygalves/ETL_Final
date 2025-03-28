"""
DAG: Database Write Raw and clean dataset for ETL final workshop
(Dinámico) – Inserción usando diccionario

Descripción:
    Este DAG se encarga de:
      1. Leer y limpiar los datos del CSV original, generando un CSV limpio.
         La limpieza reemplaza "#VALUE!" por None, convierte a numérico solo las columnas esperadas,
         normaliza columnas de texto (quitando espacios y pasando a minúsculas) y reemplaza NaN por None.
      2. Conectarse a PostgreSQL y crear la base de datos (si no existe), usando las credenciales definidas en un YAML.
      3. Crear una tabla en la base de datos con una definición fija; el nombre de la tabla se obtiene dinámicamente.
      4. Insertar (anexar) los datos del CSV limpio en la tabla creada utilizando SQLAlchemy's insert() a partir de una lista de diccionarios.
      
Variables a crear en Airflow (ejemplo):
    - data_path: "/home/ygalvis/Documents/Study/ETL_Final/dags/Data/"
    - file_name_input: "Dataset.csv"
    - db_credential_path: "/home/ygalvis/Documents/Study/ETL_Final/dags/Data/credentials.yaml"
    - raw_table_name: "Dataset"   # Este valor se usará para crear la tabla

El archivo credentials.yaml debe tener la siguiente estructura:

database:
  user: "postgres"
  password: "password"
  host: "localhost"
  port: 5432
  name: "ETL_prj"

Prueba:
    - pip install psycopg2-binary SQLAlchemy pandas PyYAML
    - $ airflow tasks test dag_02_extract job_01_clean_data 2025-01-01
"""

from datetime import datetime, timedelta
import logging
import os
import time
import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine, text, inspect, MetaData, Table, insert
from sqlalchemy.exc import SQLAlchemyError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------- Callbacks ----------------------
def task_success_callback(context):
    # primera transformacion cambia ID por Lot_ID para evitar confusion con el id de la base de datos
    task_id = context.get('task_instance').task_id
    logging.info(f"Tarea {task_id} ejecutada exitosamente.")

def task_failure_callback(context):
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')
    logging.error(f"Tarea {task_id} falló con error: {exception}")

# ---------------------- Configuración y Variables ----------------------
default_args = {
    'owner': 'UAO-YGA',
    'depends_on_past': False,
    'email': ["yoniliman.galves@uao.edu.co"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DATA_PATH = Variable.get("data_path", default_var="/home/ygalvis/Documents/Study/ETL_Final/dags/Data/")
FILE_NAME_DEFAULT = Variable.get("file_name_input", default_var="Dataset.csv")
CREDENTIALS_PATH = Variable.get("db_credential_path", default_var=os.path.join(os.path.dirname(__file__), "credentials.yaml"))
RAW_TABLE_NAME = Variable.get("raw_table_name", default_var="Dataset").lower()
CLEAN_FILE = os.path.join(DATA_PATH, "clean_" + FILE_NAME_DEFAULT)

# ---------------------- Funciones Auxiliares ----------------------
def load_config():
    """
    Lee el archivo YAML de credenciales y devuelve el diccionario.
    Se espera que el YAML tenga una clave 'database' con subclaves: user, password, host, port, name.
    """
    try:
        with open(CREDENTIALS_PATH, "r") as file:
            config = yaml.safe_load(file)
        logging.info(f"Credenciales cargadas desde {CREDENTIALS_PATH}: {config}")
        return config
    except Exception as e:
        logging.error(f"Error al leer el archivo YAML: {e}")
        raise

def clean_data(df):
    """
    Limpia el DataFrame:
      - Reemplaza "#VALUE!" por None.
      - Convierte a numérico solo las columnas esperadas (en este ejemplo, 'Value' y 'Verify').
      - Normaliza columnas de tipo object (quita espacios y pasa a minúsculas).
      - Reemplaza NaN por None.
    """
    df.replace("#VALUE!", None, inplace=True)
    numeric_columns = ['Value', 'Verify']
    df.rename(columns={'ID': 'Lot_ID'}, inplace=True)
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    text_columns = [col for col in df.columns if col not in numeric_columns]
    for col in text_columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
    df = df.where(pd.notnull(df), None)
    return df

# ---------------------- Tareas del DAG ----------------------
def job_01_clean_data(**kwargs):
    """
    Lee el CSV original, limpia el DataFrame utilizando clean_data() y guarda el CSV limpio en CLEAN_FILE.
    """
    csv_path = os.path.join(DATA_PATH, FILE_NAME_DEFAULT)
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logging.info(f"CSV original leído correctamente desde {csv_path}.")
    except Exception as e:
        logging.error(f"Error al leer el CSV original: {e}")
        raise

    try:
        df_clean = clean_data(df)
  
        df_clean.to_csv(CLEAN_FILE, index=False)
        logging.info(f"CSV limpio guardado en {CLEAN_FILE}.")
    except Exception as e:
        logging.error(f"Error al limpiar y guardar el CSV: {e}")
        raise

def create_database(**kwargs):
    """
    Conecta a PostgreSQL (base 'postgres') y crea la base de datos si no existe.
    Usa las credenciales del YAML, fuerza el nombre a minúsculas y espera 5 segundos.
    """
    try:
        config = load_config()
        db_config = config["database"]
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
        
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        conn.autocommit = True
        logging.info("Conexión a 'postgres' establecida.")
    
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                exists = cur.fetchone()
                if not exists:
                    cur.execute(f"CREATE DATABASE {db_name}")
                    logging.info(f"Database {db_name} creada exitosamente.")
                else:
                    logging.info(f"Database {db_name} ya existe.")
        except psycopg2.errors.DuplicateDatabase as e:
            logging.info(f"La base de datos '{db_name}' ya existe: {e}")
        finally:
            conn.close()
            logging.info("Conexión a 'postgres' cerrada.")
        
        time.sleep(5)
    except Exception as e:
        logging.error(f"Error en create_database: {e}")
        raise

def create_table(**kwargs):
    """
    Crea la tabla en la base de datos usando una definición fija.
    El nombre de la tabla se toma de RAW_TABLE_NAME (dinámico) y se espera 5 segundos tras la creación.
    """
    try:
        config = load_config()
        db_config = config["database"]
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
    except Exception as e:
        logging.error(f"Error al cargar credenciales en create_table: {e}")
        raise

    fixed_query = f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            "DateTime" Timestamptz,
            "Lot_ID" VARCHAR(250),
            "Prod_ID" VARCHAR(100),
            "Type" VARCHAR(10),
            "Train" VARCHAR(10),
            "Unit" VARCHAR(10),
            "Phase_ID" VARCHAR(100),
            "EU" VARCHAR(10),
            "Value" Float,
            "Verify" smallint
        );
    """
    logging.info(f"Query para crear tabla:\n{fixed_query}")

    try:
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        with engine.begin() as conn:
            conn.execute(text(fixed_query))
            logging.info(f"Tabla {RAW_TABLE_NAME} creada exitosamente.")
    except SQLAlchemyError as e:
        logging.error(f"Error en create_table: {e}")
        raise

    time.sleep(5)

def write_data(**kwargs):
    """
    Lee el CSV limpio y escribe (anexa) los datos en la tabla RAW_TABLE_NAME.
    Convierte el DataFrame a una lista de diccionarios y utiliza SQLAlchemy's insert() para insertar los registros.
    """
    csv_path = CLEAN_FILE
    try:
        df = pd.read_csv(csv_path, low_memory=False, header=0, delimiter=',')
        # Asegurarse de que los nombres de columnas sean consistentes con la tabla
        # Se espera que el CSV tenga columnas con nombres que, al limpiar, resulten en:
        # "DateTime", "lot_ID", "Prod_ID", "Type", "Train", "Unit", "Phase_ID", "EU", "Value", "Verify"
        # Si es necesario, mapea o renombra aquí:
        df.columns = [col.strip().replace(" ", "_") for col in df.columns]
        # Por ejemplo, para mapear a mayúsculas (si la tabla espera nombres exactos):
        df.rename(columns={
            "datetime": "DateTime",
            "Lot_ID": "Lot_ID",
            "prod_id": "Prod_ID",
            "type": "Type",
            "train": "Train",
            "unit": "Unit",
            "phase_id": "Phase_ID",
            "eu": "EU",
            "value": "Value",
            "verify": "Verify"
        }, inplace=True)
        logging.info(f"CSV limpio leído correctamente desde {csv_path}.")
    except Exception as e:
        logging.error(f"Error en write_data al leer CSV: {e}")
        raise

    # Convertir NaN a None
    df = df.where(pd.notnull(df), None)

    try:
        config = load_config()
        db_config = config["database"]
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
    except Exception as e:
        logging.error(f"Error al cargar credenciales en write_data: {e}")
        raise

    try:
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        insp = inspect(engine)
        if RAW_TABLE_NAME.lower() not in [t.lower() for t in insp.get_table_names()]:
            raise Exception(f"La tabla {RAW_TABLE_NAME} no existe. Créala antes de insertar datos.")
    except Exception as e:
        logging.error(f"Error al inspeccionar la base de datos: {e}")
        raise

    try:
        records = df.to_dict(orient='records')
        metadata = MetaData(bind=engine)
        table = Table(RAW_TABLE_NAME, metadata, autoload_with=engine)
        with engine.begin() as conn:
            conn.execute(insert(table), records)
        logging.info("Datos insertados exitosamente en la tabla mediante insert().")
    except Exception as e:
        logging.error(f"Error en write_data al insertar datos: {e}")
        raise

# ---------------------- Definición del DAG ----------------------
with DAG(
    'dag_02_extract',
    description="DAG para crear la base de datos y escribir datos desde CSV a la tabla (dinámica) – Inserción usando diccionario",
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Database', 'ETL', 'Write', 'raw_dataset']
) as dag_db_write:

    job_01_clean_data_task = PythonOperator(
        task_id='job_01_clean_data',
        python_callable=job_01_clean_data,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_02_create_database = PythonOperator(
        task_id='job_02_create_database',
        python_callable=create_database,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_03_create_table = PythonOperator(
        task_id='job_03_create_table',
        python_callable=create_table,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_04_write_data = PythonOperator(
        task_id='job_04_write_data',
        python_callable=write_data,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_01_clean_data_task >> job_02_create_database >> job_03_create_table >> job_04_write_data

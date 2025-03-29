"""
DAG: Extraction for CSV for ETL final workshop

Descripción:
    Este DAG define cinco tareas:
      - load_config: Carga el archivo YAML de configuración y valida que tenga la estructura esperada.
      - clean_old_files: Limpia la carpeta eliminando archivos con extensiones definidas 
                         que sean más antiguos que el número de días especificado.
      - check_file_name: Verifica y retorna el nombre del archivo a procesar, obteniéndolo de la configuración
                         o usando "Dataset.csv" como valor por defecto.
      - get_csv: Descarga un archivo CSV desde una URL, utilizando el nombre determinado.
      - profiling: Lee el CSV descargado, genera un reporte de calidad de datos usando ydata_profiling y
                   lo guarda en un archivo HTML, incluyendo en el título el nombre completo (con extensión) del archivo.

Requisitos:
    - Airflow debe estar correctamente configurado.
    - Las dependencias necesarias (Airflow, ydata_profiling, pandas, PyYAML, requests, etc.) deben estar instaladas.

El archivo configuration.yaml debe estar en la carpeta *dags/credentials* y tener la siguiente estructura (usa direcciones absolitas):
    
configuration:
  data_path: "~/Documents/Study/ETL_Final/dags/data"
  file_name_input: "Dataset.csv"
  file_extension_input: "html, csv, txt"
  report_retention_days: 7
  url_file: "https://raw.githubusercontent.com/ygalves/ETL_project/refs/heads/main/Dataset/caso%20etl.csv"
  raw_table_name: "etl_dataset"
  transformed_table_name: "etl_transformed"
  eu_table: "eu"
  events_table: "events"
  lots_table: "lots"
  phases_table: "phases"


Prueba:
    - pip install psycopg2-binary SQLAlchemy pandas PyYAML
    - $ airflow tasks test dag_01_getdata job_00_load_config 2025-01-01
"""


from datetime import datetime, timedelta
import logging
import os
import time
import pytz
import pandas as pd
import yaml
import requests

from ydata_profiling import ProfileReport
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------- Callbacks ----------------------
def task_success_callback(context):
    task_id = context.get('task_instance').task_id
    logging.info(f"Tarea {task_id} ejecutada exitosamente.")

def task_failure_callback(context):
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')
    logging.error(f"Tarea {task_id} falló con error: {exception}")

# ---------------------- Tarea: Cargar Configuración ----------------------
def load_config(**kwargs):
    """
    Lee el archivo YAML de configuración y devuelve el diccionario correspondiente.
    Se espera que el YAML tenga una clave 'configuration' con las siguientes subclaves obligatorias:
    - data_path
    - file_name_input
    - file_extension_input
    - report_retention_days
    - url_file
    - raw_table_name

    Si ocurre algún error (archivo no encontrado, estructura incorrecta, etc.), se registra y lanza la excepción.
    """
    try:
        CONFIG_PATH = Variable.get("config_path", default_var=os.path.join(os.path.dirname(__file__), 'credentials/configuration.yaml'))
        with open(CONFIG_PATH, "r") as file:
            config_yaml = yaml.safe_load(file)
        logging.info(f"Configuración cargada desde {CONFIG_PATH}: {config_yaml}")
        cfg = config_yaml.get("configuration", {})
        # Validar que existan las claves obligatorias
        required_keys = ["data_path", "file_name_input", "file_extension_input", "report_retention_days", "url_file", "raw_table_name"]
        for key in required_keys:
            if key not in cfg:
                raise KeyError(f"La clave requerida '{key}' no se encuentra en el archivo de configuración.")
        # Asegurar que el directorio data_path exista
        os.makedirs(cfg["data_path"], exist_ok=True)
        return cfg
    except Exception as e:
        logging.error(f"Error al cargar configuración: {e}")
        raise

# ---------------------- Tarea: Limpieza de Archivos Antiguos ----------------------
def clean_old_files(**kwargs):
    """
    Limpia la carpeta definida en la configuración eliminando archivos con extensiones especificadas
    que sean más antiguos que report_retention_days.
    """
    try:
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids="job_00_load_config")
        if not config:
            raise Exception("No se pudo cargar la configuración.")
        data_path = config["data_path"]
        file_extension = config["file_extension_input"]
        try:
            retention_days = int(config["report_retention_days"])
        except ValueError:
            retention_days = 7

        retention_seconds = retention_days * 24 * 60 * 60  # Convertir días a segundos
        current_time = time.time()
        extensions = [ext.strip().lower() for ext in file_extension.split(',')]
        logging.info(f"Iniciando limpieza en {data_path} para extensiones {extensions} con retención de {retention_days} días.")

        for filename in os.listdir(data_path):
            file_ext = filename.split('.')[-1].lower() if '.' in filename else ""
            if file_ext in extensions:
                filepath = os.path.join(data_path, filename)
                if os.path.isfile(filepath) and (current_time - os.path.getmtime(filepath) > retention_seconds):
                    logging.info(f"Eliminando {filepath} (modificado: {time.ctime(os.path.getmtime(filepath))})")
                    os.remove(filepath)
        logging.info("Limpieza completada.")
    except Exception as e:
        logging.error(f"Error en clean_old_files: {e}")
        raise

# ---------------------- Tarea: Verificar Nombre de Archivo ----------------------
def check_file_name(**kwargs):
    """
    Obtiene el nombre del archivo a procesar usando el valor de la configuración.
    """
    try:
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids="job_00_load_config")
        if not config:
            raise Exception("No se pudo cargar la configuración.")
        file_name = config["file_name_input"]
        logging.info(f"Nombre de archivo a procesar: {file_name}")
        return file_name
    except Exception as e:
        logging.error(f"Error en check_file_name: {e}")
        raise

# ---------------------- Tarea: Descargar CSV ----------------------
def get_csv(**kwargs):
    """
    Descarga el archivo CSV desde la URL definida en la configuración y lo guarda en data_path
    con el nombre obtenido de la tarea check_file_name.
    """
    try:
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids="job_00_load_config")
        if not config:
            raise Exception("No se pudo cargar la configuración.")
        data_path = config["data_path"]
        url_file = config["url_file"]
        file_name = ti.xcom_pull(task_ids="job_02_check_file_name")
        if not file_name:
            file_name = config["file_name_input"]
        full_path = os.path.join(data_path, file_name)
        logging.info(f"Descargando CSV desde {url_file} a {full_path}")
        response = requests.get(url_file)
        if response.status_code == 200:
            with open(full_path, "wb") as f:
                f.write(response.content)
            logging.info("Descarga completada.")
        else:
            raise Exception(f"Error en la descarga, código de estado: {response.status_code}")
    except Exception as e:
        logging.error(f"Error en get_csv: {e}")
        raise

# ---------------------- Tarea: Generar Reporte de Calidad ----------------------
# Configuración de zona horaria y fechas (se definen globalmente, ya que no dependen de la configuración)
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
NOW = datetime.now(TZ).strftime('%Y/%m/%d %H:%M:%S')

def _profile(**kwargs):
    """
    Lee el CSV descargado, genera un reporte de calidad de datos y lo guarda en HTML.
    El título incluye el nombre del archivo y el timestamp de ejecución.
    """
    try:
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids="job_00_load_config")
        if not config:
            raise Exception("No se pudo cargar la configuración.")
        data_path = config["data_path"]
        file_name = ti.xcom_pull(task_ids="job_02_check_file_name")
        if not file_name:
            file_name = config["file_name_input"]
        csv_path = os.path.join(data_path, file_name)
        df = pd.read_csv(csv_path, low_memory=False)
        
        report_title = f"Data Quality Report for {file_name} - Execution: {TODAY}_{NOW}"
        profile = ProfileReport(df, title=report_title)
        
        file_base = file_name.rsplit('.', 1)[0]
        output_file = os.path.join(data_path, f"data_quality_report_{file_base}_{TODAY}.html")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        profile.to_file(output_file)
        logging.info(f"Reporte generado en {output_file}")
    except Exception as e:
        logging.error(f"Error en _profile: {e}")
        raise

# ---------------------- Definición del DAG ----------------------
default_args = {
    'owner': 'my',
    'depends_on_past': False,
    'email': ["myuser@myemail.co"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_01_getdata',
    description="Get dataset from github raw csv file",
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Raw Dataset', 'Get Data', 'Github raw csv']
) as dag:

    # Tarea 0: Cargar configuración desde YAML
    job_00_load_config = PythonOperator(
        task_id='job_00_load_config',
        python_callable=load_config,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tarea 1: Limpiar archivos antiguos
    job_01_clean_old_files = PythonOperator(
        task_id='job_01_clean_old_files',
        python_callable=clean_old_files,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tarea 2: Obtener nombre del archivo
    job_02_check_file_name = PythonOperator(
        task_id='job_02_check_file_name',
        python_callable=check_file_name,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tarea 3: Descargar CSV
    job_03_get_csv = PythonOperator(
        task_id='job_03_get_csv',
        python_callable=get_csv,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tarea 4: Generar reporte de calidad (profiling)
    job_04_profiling = PythonOperator(
        task_id='job_04_profiling',
        python_callable=_profile,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Secuencia de ejecución:
    job_00_load_config >> job_01_clean_old_files >> job_02_check_file_name >> job_03_get_csv >> job_04_profiling

"""
DAG: Extraction for ETL final workshop

Descripción:
    Este DAG define cuatro tareas:
      - clean_old_files: Tarea Python que limpia la carpeta eliminando archivos con extensiones 
                         definidas (ej.: html, csv, txt) que sean más antiguos que el número de días especificado.
      - check_file_name: Tarea Python que verifica y retorna el nombre del archivo a procesar,
                         obteniéndolo de la variable de Airflow "file_name_input" o usando "Dataset.csv"
                         como valor por defecto.
      - get_csv: Tarea Bash que descarga un archivo CSV desde una URL, utilizando el nombre determinado.
      - profiling: Tarea Python que lee el CSV descargado, genera un reporte de calidad de datos usando
                   ydata_profiling y lo guarda en un archivo HTML, incluyendo en el título el nombre
                   completo (con extensión) del archivo.
    
Requisitos:
    - Airflow debe estar correctamente configurado.
    - Las dependencias necesarias (Airflow, ydata_profiling, pandas, etc.) deben estar instaladas.
    
Variables a crear en Airflow:
    - data_path: Ruta absoluta del directorio (ejemplo: "/home/ygalvis/Documents/Study/ETL_Final/dags/Data/")
    - file_name_input: Nombre del archivo CSV (ejemplo: "Dataset.csv")
    - file_extension_input: Extensión o extensiones de archivos a limpiar (ejemplo: "html, csv, txt")
    - report_retention_days: Número de días de retención para limpiar archivos antiguos (ejemplo: "7")

Prueba:
    - pip install apache-airflow[cncf.kubernetes]
    - $ airflow tasks test dag_extract clean_old_files 2025-01-01
"""

from datetime import datetime, timedelta
import logging
import os
import time
import pytz
import pandas as pd
from ydata_profiling import ProfileReport

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configuración de zona horaria y fecha actual
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
NOW = datetime.now(TZ).strftime('%Y/%m/%d %H:%M:%S')

# Se obtienen las variables de Airflow o se utilizan valores por defecto
DATA_PATH = Variable.get("data_path", default_var="/home/ygalvis/Documents/Study/ETL_Final/dags/Data/")
FILE_NAME_DEFAULT = Variable.get("file_name_input", default_var="Dataset.csv")
# La variable file_extension_input se espera en formato CSV, p.ej.: "html, csv, txt"
FILE_EXTENSION = Variable.get("file_extension_input", default_var="html")
try:
    REPORT_RETENTION_DAYS = int(Variable.get("report_retention_days", default_var=7))
except ValueError:
    REPORT_RETENTION_DAYS = 7

# URL de descarga (valor fijo en este ejemplo)
URL = 'https://raw.githubusercontent.com/ygalves/ETL_project/refs/heads/main/Dataset/caso%20etl.csv'

# Argumentos predeterminados para el DAG
dag_args = {
    'owner': 'UAO-YGA',
    'depends_on_past': False,
    'email': ["yoniliman.galves@uao.edu.co"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def clean_old_files(**kwargs):
    """
    Tarea para limpiar la carpeta definida en DATA_PATH.
    Elimina archivos que tengan alguna de las extensiones especificadas en FILE_EXTENSION
    y que sean más antiguos que REPORT_RETENTION_DAYS días.
    La función normaliza los nombres de extensiones (quitando espacios y usando minúsculas).
    """
    retention_seconds = REPORT_RETENTION_DAYS * 24 * 60 * 60  # Convertir días a segundos
    now = time.time()
    # Convertir la variable de extensiones (CSV) en una lista de extensiones normalizadas
    extensions = [ext.strip().lower() for ext in FILE_EXTENSION.split(',')]
    
    logging.info(f"Iniciando limpieza en {DATA_PATH} para archivos con extensiones {extensions} antiguos a {REPORT_RETENTION_DAYS} días.")
    
    # Iterar sobre todos los archivos en DATA_PATH
    for filename in os.listdir(DATA_PATH):
        # Extraer la extensión del archivo en minúsculas
        file_ext = filename.split('.')[-1].lower() if '.' in filename else ""
        if file_ext in extensions:
            filepath = os.path.join(DATA_PATH, filename)
            if os.path.isfile(filepath):
                file_mtime = os.path.getmtime(filepath)
                if now - file_mtime > retention_seconds:
                    logging.info(f"Eliminando {filepath} (modificado: {time.ctime(file_mtime)})")
                    os.remove(filepath)
    logging.info("Limpieza completada.")
    pass

def check_file_name(**kwargs):
    """
    Tarea para obtener el nombre del archivo.
    Revisa la variable de Airflow "file_name_input" y, si no está definida, usa el valor por defecto.
    """
    file_name = Variable.get("file_name_input", default_var=FILE_NAME_DEFAULT)
    logging.info(f"Nombre de archivo a procesar: {file_name}")
    return file_name

def _profile(**kwargs):
    ti = kwargs['ti']
    # Obtener el timestamp de ejecución (execution_date es un objeto datetime)
    file_name = ti.xcom_pull(task_ids="check_file_name")
    if not file_name:
        file_name = FILE_NAME_DEFAULT
    csv_path = os.path.join(DATA_PATH, file_name)
    df = pd.read_csv(csv_path, low_memory=False)
    
    # El título incluye el nombre del archivo y el timestamp de ejecución
    report_title = f"Data Quality Report for {file_name} - Execution: {TODAY}_{NOW}"
    profile = ProfileReport(df, title=report_title)
    
    file_base = file_name.rsplit('.', 1)[0]
    output_file = os.path.join(DATA_PATH, f"data_quality_report_{file_base}_{TODAY}.html")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    profile.to_file(output_file)
    logging.info(f"Reporte generado en {output_file}")

def Data_clean_task(
    csv_path = os.path.join(DATA_PATH, file_name)
    df = pd.read_csv(csv_path, low_memory=False, header=0,delimiter=',') #espicifico qu etiene encabezados y esta separado por ;
)

# Definición del DAG utilizando el contexto "with"
with DAG(
    'dag_extract',
    description="Extract dag for ETL final workshop",
    default_args=dag_args,
    schedule='@once',                   # Se ejecuta automáticamente (ajustable a otro schedule)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Extract', 'ETL_Final', 'Final Project']
) as dag:

    # Tarea de limpieza: elimina archivos con las extensiones definidas que sean más antiguos que el tiempo de retención
    clean_old_files_task = PythonOperator(
        task_id='clean_old_files',
        python_callable=clean_old_files,
        provide_context=True
    )

    # Tarea para determinar el nombre del archivo.
    check_file_name_task = PythonOperator(
        task_id='check_file_name',
        python_callable=check_file_name,
        provide_context=True
    )

    # Tarea para descargar el CSV usando curl, utilizando el nombre obtenido de check_file_name.
    get_csv = BashOperator(
        task_id='get_csv',
        bash_command='curl -o {{ params.path }}{{ ti.xcom_pull(task_ids="check_file_name") }} {{ params.url }}',
        params={
            'path': DATA_PATH,
            'url': URL,
        }
    )

    # Tarea para generar el reporte de calidad de datos a partir del CSV descargado.
    profiling = PythonOperator(
        task_id='profiling',
        python_callable=_profile,
        provide_context=True
    )

    # Limpiar los datos .
    Data_clean_task = PythonOperator(
        task_id = 'Data_clean_task',
        python_callable = 'Data_clean',
    )
    

    # Secuencia de ejecución:
    # Primero se limpia la carpeta, luego se determina el nombre del archivo,
    # se descarga el CSV y finalmente se genera el reporte.
    clean_old_files_task >> check_file_name_task >> get_csv >> profiling

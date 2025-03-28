"""
DAG: ETL Transform & Load - Separación en tablas Eu, Events, Lots y Phases

Descripción:
    Este DAG se encarga de:
      1. Cargar la configuración general y las credenciales desde dos archivos YAML.
      2. Extraer datos de la tabla de origen (raw_table_name) de la base de datos.
      3. Transformar y limpiar los datos extraídos.
      4. Dividir el DataFrame transformado en cuatro subconjuntos correspondientes a:
         - Eu: Engineering Units.
         - Events: Event Log.
         - Lots: Produced Lots.
         - Phases: Phases.
      5. Crear (si no existen) y cargar cada uno de estos subconjuntos en sus respectivas tablas usando inserciones basadas en diccionarios.
      
Variables a crear en Airflow (ejemplo):
    - config_path: Ruta al YAML de configuración general (por ejemplo, "/ruta/al/configuration.yaml")
    - db_credential_path: Ruta al YAML de credenciales (por ejemplo, "/ruta/al/credentials.yaml")
    
El archivo configuration.yaml debe tener la siguiente estructura:
    
    configuration:
      data_path: "/ruta/al/Data/"
      file_name_input: "Dataset.csv"
      file_extension_input: "csv, txt, html"
      report_retention_days: 7
      url_file: "https://ruta/al/archivo.csv"
      raw_table_name: "etl_dataset"
      transformed_table_name: "etl_transformed"
      eu_table: "eu"
      events_table: "events"
      lots_table: "lots"
      phases_table: "phases"

El archivo credentials.yaml debe tener la siguiente estructura:

    database:
      user: "postgres"
      password: "password"
      host: "localhost"
      port: 5432
      name: "ETL_prj"

Requisitos:
    - pip install psycopg2-binary SQLAlchemy pandas PyYAML
"""

from datetime import datetime, timedelta
import logging
import os
import io
import pandas as pd
import yaml
import numpy as np


from sqlalchemy import create_engine, MetaData, Table, insert, text
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

class DBAPIConnectionWrapper:
    def __init__(self, dbapi_conn):
        self.dbapi_conn = dbapi_conn
    def cursor(self):
        return self.dbapi_conn.cursor()
    def commit(self):
        return self.dbapi_conn.commit()
    def close(self):
        return self.dbapi_conn.close()
    def __getattr__(self, name):
        return getattr(self.dbapi_conn, name)

# ---------------------- Callbacks ----------------------
def task_success_callback(context):
    ti = context.get('task_instance')
    message = "Tarea completada satisfactoriamente"
    ti.xcom_push(key="status", value=message)
    logging.info(f"Tarea {ti.task_id} completada satisfactoriamente.")

def task_failure_callback(context):
    ti = context.get('task_instance')
    error = context.get('exception')
    message = f"Tarea {ti.task_id} falló con error: {error}"
    ti.xcom_push(key="status", value=message)
    logging.error(message)

# ---------------------- Tarea: Cargar Configuración General ----------------------
def load_config(**kwargs):
    try:
        config_path = Variable.get("config_path",
            default_var=os.path.join(os.path.dirname(__file__), "credentials", "configuration.yaml"))
        with open(config_path, "r") as file:
            config_yaml = yaml.safe_load(file)
        logging.info(f"Configuración general cargada desde {config_path}: {config_yaml}")
        config = config_yaml.get("configuration")
        if not config:
            raise KeyError("La clave 'configuration' no se encuentra en el archivo YAML.")
        required_keys = ["data_path", "file_name_input", "file_extension_input", 
                         "report_retention_days", "url_file", "raw_table_name",
                         "transformed_table_name", "eu_table", "events_table", "lots_table", "phases_table"]
        for key in required_keys:
            if key not in config:
                raise KeyError(f"La clave requerida '{key}' no se encuentra en la sección 'configuration'.")
        os.makedirs(config["data_path"], exist_ok=True)
        return config
    except Exception as e:
        logging.error(f"Error al cargar la configuración general: {e}")
        raise

# ---------------------- Tarea: Cargar Credenciales ----------------------
def load_db_config(**kwargs):
    try:
        credentials_path = Variable.get("db_credential_path",
            default_var=os.path.join(os.path.dirname(__file__), "credentials", "credentials.yaml"))
        with open(credentials_path, "r") as file:
            config_yaml = yaml.safe_load(file)
        logging.info(f"Credenciales cargadas desde {credentials_path}: {config_yaml}")
        if "database" not in config_yaml:
            raise KeyError("La sección 'database' es obligatoria en el archivo de credenciales.")
        required_db_keys = ["user", "password", "host", "port", "name"]
        for key in required_db_keys:
            if key not in config_yaml["database"]:
                raise KeyError(f"La clave requerida '{key}' no se encuentra en la sección 'database'.")
        return config_yaml["database"]
    except Exception as e:
        logging.error(f"Error al cargar las credenciales de la base de datos: {e}")
        raise

# --------------------- Constraints 
def add_constraint_if_not_exists(conn, constraint_name, alter_sql):
    result = conn.execute(text("SELECT 1 FROM pg_constraint WHERE conname = :conname"), {"conname": constraint_name}).fetchone()
    if not result:
        conn.execute(text(alter_sql))

def create_constraints(conn):
    add_constraint_if_not_exists(
        conn,
        'fk_events_to_phases',
        "ALTER TABLE Phases ADD CONSTRAINT FK_Events_TO_Phases FOREIGN KEY (Events_LogID) REFERENCES Events (LogID)"
    )
    add_constraint_if_not_exists(
        conn,
        'fk_lots_to_phases',
        "ALTER TABLE Phases ADD CONSTRAINT FK_Lots_TO_Phases FOREIGN KEY (Lots_LogID) REFERENCES Lots (LogID)"
    )
    add_constraint_if_not_exists(
        conn,
        'fk_eu_to_phases',
        "ALTER TABLE Phases ADD CONSTRAINT FK_EU_TO_Phases FOREIGN KEY (EU_LogID) REFERENCES EU (LogID)"
    )


# ---------------------- Tarea: Crear Tablas desde Archivo SQL ----------------------
def create_tables_from_file(**kwargs):
    ti = kwargs['ti']
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    if not db_config:
        raise Exception("No se encontraron credenciales de la base de datos.")
    
    try:
        sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
        with open(sql_file_path, 'r') as file:
            ddl = file.read()
        logging.info(f"Contenido del archivo SQL:\n{ddl}")

        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
        engine_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logging.info(f"Engine URI para crear tablas: {engine_uri}")
        engine = create_engine(engine_uri)
        
        # Usar conexión con autocommit
        with engine.connect().execution_options(autocommit=True) as conn:
            # Ejecutar el archivo SQL completo (sin dividirlo)
            conn.execute(text(ddl))
            # Agregar constraints condicionalmente
            create_constraints(conn)
        logging.info("Tablas y constraints creados (o ya existentes) correctamente.")
    except Exception as e:
        error_detail = f"Error al crear tablas. Engine: {engine_uri if 'engine_uri' in locals() else 'No definido'}, Error: {str(e)}"
        ti.xcom_push(key="create_tables_error", value=error_detail)
        logging.error(error_detail)
        raise


# ---------------------- Tarea: Extraer Datos ----------------------
def extract_data(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids="job_00_load_config")
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    if not config or not db_config:
        error_msg = "No se pudo cargar la configuración general o las credenciales."
        ti.xcom_push(key="extract_error", value=error_msg)
        raise Exception(error_msg)
    try:
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
        engine_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logging.info(f"Cadena del engine: {engine_uri}")
        engine = create_engine(engine_uri)
        query = f"SELECT * FROM {config['raw_table_name']};"
        logging.info(f"Cadena de la query: {query}")
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(query, con=conn)
            logging.info(f"Shape dataframe read from table: {df.shape}")
        finally:
            conn.close()
        logging.info(f"Se extrajeron {len(df)} registros de la tabla {config['raw_table_name']}.")
        return df.to_json(orient='split')
    except Exception as e:
        error_detail = f"Error al extraer datos. Engine: {engine_uri if 'engine_uri' in locals() else 'No definido'}, Query: {query if 'query' in locals() else 'No definida'}, Error: {str(e)}"
        ti.xcom_push(key="extract_error", value=error_detail)
        logging.error(error_detail)
        raise

# ---------------------- Tarea: Transformar Datos ----------------------
def transform_data(**kwargs):
    ti = kwargs['ti']
    extracted_json = ti.xcom_pull(task_ids="job_03_extract_data")
    if not extracted_json:
        raise Exception("No se pudo extraer el DataFrame de la tabla de origen.")
    try:
        df = pd.read_json(extracted_json, orient='split')
        logging.info(f"Datos a transformar: {len(df)} registros.")
        df.replace("#VALUE!", None, inplace=True)
        numeric_columns = ['Value', 'Verify']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        text_columns = [col for col in df.columns if df[col].dtype == object]
        for col in text_columns:
            df[col] = df[col].apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
        if 'ID' in df.columns:
            df.rename(columns={'ID': 'Lot_ID'}, inplace=True)
        logging.info("Transformación completada.")
        return df.to_json(orient='split')
    except Exception as e:
        logging.error(f"Error en transform_data: {e}")
        raise

# ---------------------- Tarea: Dividir Datos en Subconjuntos ----------------------
def split_data(**kwargs):
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids="job_04_transform_data")
    if not transformed_json:
        raise Exception("No se pudo obtener el DataFrame transformado.")
    try:
        logging.info("get transformed_json")
        df = pd.read_json(transformed_json, orient='split')
        logging.info(f"info df: {df.info()}")
        
        # --- EU ---
        logging.info("EU Dataframe")
        df_eu = df['EU'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).to_frame()
        df_eu = df_eu.rename(columns={'EU': 'EU_ID'})
        df_eu['EU_name'] = 'EU_' + df_eu.index.astype(str)
        # Diccionario de mapeo de unidades a descripciones
        mapping_desc = {'seg': 'waittime','ph': 'acidity','kg/l': 'density','kg': 'weight','cp': 'viscosity','c': 'temperature','': 'no assigned'}
        # Diccionario de mapeo de unidades a tipo
        mapping_type = {'seg': 'phase','ph': 'control','kg/l': 'control','kg': 'phase','cp': 'control','c': 'phase','': 'phase'}
        df_eu['Description'] = df_eu['EU_ID'].map(mapping_desc)
        df_eu['Type'] = df_eu['EU_ID'].map(mapping_type)
        logging.info(f"info eu: {df_eu.info()}")
        
        # --- Events ---
        logging.info("Events Dataframe")
        df_events = df['DateTime'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).to_frame()
        # Definir que la columna 'DateTime' sea en formato datetime
        df_events["DateTime"] = pd.to_datetime(df_events["DateTime"])
        # divisiones del año
        df_events["Year"]      = df_events["DateTime"].dt.year      # Año
        df_events["Quarter"]   = df_events["DateTime"].dt.quarter   # cada trimestre
        df_events["Month"]     = df_events["DateTime"].dt.month    # Mes
        df_events["MonthName"] = df_events["DateTime"].dt.month_name()    # Nombre del día del mes
        # divisiones del mes
        df_events["Week"]      = df_events["DateTime"].dt.isocalendar().week
        df_events["WeekDay"]   = df_events["DateTime"].dt.weekday  # Día de la semana (0 = Lunes, 6 = Domingo)
        df_events["DayOfYear"] = df_events["DateTime"].dt.dayofyear  # Día del año
        df_events["Day"]       = df_events["DateTime"].dt.day  # Día del mes
        df_events["DayName"]   = df_events["DateTime"].dt.day_name()   # Nombre del día de la semana
        # divisiones del día 
        df_events["Hour"]      = df_events["DateTime"].dt.hour
        df_events["Minute"]    = df_events["DateTime"].dt.minute
        df_events["Second"]    = df_events["DateTime"].dt.second
        # Definir el turno de trabajo
        df_events["Shift_8H"] = np.select([(df_events["Hour"] >= 6) & (df_events["Hour"] < 14),(df_events["Hour"] >= 14) & (df_events["Hour"] < 22)],[1, 2],default=3)
        # Crear formato de fecha
        df_events["Formatted_Timestamp"] = df_events["DateTime"].dt.strftime('%Y%m%d%H%M%S')
        logging.info(f"info events: {df_events.info()}")
        
        # --- Lots ---
        logging.info("Lots - df_prods Dataframe")
        # filtra únicamente las columnas categóricas necesarias
        # llevar a poructos los registros ddistintos del dataset original
        df_prods = df['Prod_ID'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).to_frame()
        # Crear una nueva columna 'Producto' con el formato "producto_x" donde x es el índice
        df_prods['Product_name'] = 'Product_' + df_prods.index.astype(str)
        logging.info(f"info prods: {df_prods.info()}")
        
        logging.info("Lots - df_sub Dataframe")
        df_sub = df[['id', 'DateTime', 'Type', 'Train', 'Prod_ID']].copy().sort_values(by=['DateTime'], ascending=True)
        # Asegúrate de que 'DateTime' esté en formato datetime
        df_sub['DateTime'] = pd.to_datetime(df_sub['DateTime'])
        # De nuevo cambio de nombre columna ID  para mejorar la lectura
        df_sub = df_sub.rename(columns={'id': 'Lot_ID'})
        # De nuevo cambio de nombre columna ID  para mejorar la lectura
        df_sub = df_sub.rename(columns={'Train': 'Train_ID'})
        # se decide renombrar los productos, esto ayuda a mejorar la lectura de los datos
        df_sub = df_sub.merge(df_prods, left_on='Prod_ID', right_on='Prod_ID', how='left')
        # Diccionario de mapeo de los trenes de producción
        mapping_train = {'a': 'Train_1','b': 'Train_2','c': 'Train_3','d': 'Train_4','e': 'Train_5','f': 'Train_6','g': 'Train_7','h': 'Train_8'}
        df_sub['Train'] = df_sub['Train_ID'].map(mapping_train)
        logging.info(f"Lots - info df_sub: {df_sub.info()}")
        
        logging.info("df_lots Dataframe")
        # Agrupar por 'ID' y obtener la fecha mínima (primera) y la fecha máxima (última)
        df_lots = df_sub.groupby('Lot_ID', as_index=False).agg(
            Type         = ('Type', 'first'),
            Train_ID     = ('Train_ID', 'first'),
            Train        = ('Train', 'first'),
            Prod_ID      = ('Prod_ID', 'first'),
            Product_name = ('Product_name', 'first'),
            First_Date   = ('DateTime', 'min'),
            Last_Date    = ('DateTime', 'max')
        )
        # Ordenar el DataFrame resultante de forma descendente por 'first_date'
        df_lots = df_lots.sort_values(by='First_Date', ascending=True).reset_index(drop=True)
        # Calcular la duración como la diferencia entre last_date y first_date
        df_lots['Duration'] = df_lots['Last_Date'] - df_lots['First_Date']
        # Calcular la duración como la diferencia entre last_date y first_date
        df_lots['Formatted_Timestamp'] = df_lots["First_Date"].dt.strftime('%Y%m%d%H%M%S')
        logging.info(f"Lots - info lots: {df_lots.info()}")
        
        # --- Phases ---
        logging.info("df_phases Dataframe")
        # llevar a eventos los registros de tiempo del dataset original 
        df_phases =  df[['DateTime', 'Unit', 'Phase_ID','Value','EU','id']].copy().sort_values(by=['DateTime'], ascending=True)        
        df_phases["DateTime"] = pd.to_datetime(df_phases["DateTime"])
        # Calcular la duración como la diferencia entre last_date y first_date
        df_phases['Formatted_Timestamp'] = df_phases["DateTime"].dt.strftime('%Y%m%d%H%M%S')
        logging.info(f"info lots: {df_phases.info()}")
        
        result = {
            "eu": df_eu.head(10).to_json(orient='split'),
            "events": df_events.head(10).to_json(orient='split'),
            "lots": df_lots.head(10).to_json(orient='split'),
            "phases": df_phases.head(10).to_json(orient='split')
        }
        logging.info("División de datos en subconjuntos completada.")
        return result
    except Exception as e:
        logging.error(f"Error en split_data: {e}")
        raise

# ---------------------- Tarea: Cargar Datos en Tablas Específicas ----------------------
def load_table(table_key, json_data, db_config, dest_table, **kwargs):
    """
    Convierte el JSON a DataFrame e inserta los registros en la tabla destino usando pandas.to_sql.
    Se crea una conexión con engine.connect(), se extrae la conexión DBAPI subyacente y se la envuelve
    en un wrapper que expone el método cursor(). De esta forma, pandas puede usarla correctamente.
    """
    ti = kwargs.get('ti', None)
    try:
        # Leer el JSON usando StringIO para evitar FutureWarning
        df = pd.read_json(io.StringIO(json_data), orient='split')
        
        # Extraer parámetros de conexión
        db_user = db_config["user"]
        db_password = db_config["password"]
        db_host = db_config["host"]
        db_port = db_config["port"]
        db_name = str(db_config["name"]).lower()
        engine_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logging.info(f"Engine URI: {engine_uri}")
        
        # Crear el engine de SQLAlchemy
        engine = create_engine(engine_uri)
        
        # Crear la conexión con un bloque 'with'
        
        with engine.connect() as conn:
            # Extraer la conexión DBAPI subyacente y envolverla
            raw_conn = conn.connection
            wrapped_conn = DBAPIConnectionWrapper(raw_conn)
            
            logging.error(df.head(1))
            logging.error(df.shape)
            # Usar el objeto envuelto en to_sql
            df.to_sql(dest_table, con=wrapped_conn, if_exists='append', index=False, method='multi')
            wrapped_conn.commit()
            wrapped_conn.close()
        
        logging.info(f"Datos cargados en la tabla {dest_table} para el conjunto {table_key}.")
    except Exception as e:
        error_detail = (
            f"Error al cargar la tabla {dest_table} para el conjunto {table_key}. "
            f"Engine: {engine_uri if 'engine_uri' in locals() else 'No definido'}, Error: {str(e)}"
        )
        if ti:
            ti.xcom_push(key="load_table_error", value=error_detail)
        logging.error(error_detail)
        raise

def load_eu(**kwargs):
    ti = kwargs['ti']
    split_dict = ti.xcom_pull(task_ids="job_05_split_data")
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    global_config = ti.xcom_pull(task_ids="job_00_load_config")
    if not split_dict or not db_config or not global_config:
        raise Exception("Error al obtener datos para cargar en tablas.")
    load_table("eu", split_dict["eu"], db_config, global_config["eu_table"], ti=ti)

def load_events(**kwargs):
    ti = kwargs['ti']
    split_dict = ti.xcom_pull(task_ids="job_05_split_data")
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    global_config = ti.xcom_pull(task_ids="job_00_load_config")
    if not split_dict or not db_config or not global_config:
        raise Exception("Error al obtener datos para cargar en tablas.")
    load_table("events", split_dict["events"], db_config, global_config["events_table"], ti=ti)

def load_lots(**kwargs):
    ti = kwargs['ti']
    split_dict = ti.xcom_pull(task_ids="job_05_split_data")
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    global_config = ti.xcom_pull(task_ids="job_00_load_config")
    if not split_dict or not db_config or not global_config:
        raise Exception("Error al obtener datos para cargar en tablas.")
    load_table("lots", split_dict["lots"], db_config, global_config["lots_table"], ti=ti)

def load_phases(**kwargs):
    ti = kwargs['ti']
    split_dict = ti.xcom_pull(task_ids="job_05_split_data")
    db_config = ti.xcom_pull(task_ids="job_01_load_db_config")
    global_config = ti.xcom_pull(task_ids="job_00_load_config")
    if not split_dict or not db_config or not global_config:
        raise Exception("Error al obtener datos para cargar en tablas.")
    load_table("phases", split_dict["phases"], db_config, global_config["phases_table"], ti=ti)

# ---------------------- Definición del DAG ----------------------
default_args = {
    'owner': 'UAO-YGA',
    'depends_on_past': False,
    'email': ["yoniliman.galves@uao.edu.co"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_03_transform',
    description="DAG que extrae, transforma y carga datos en tablas (EU, Events, Lots, Phases)",
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'Transform', 'Load', 'Database']
) as dag_etl:

    job_00_load_config = PythonOperator(
        task_id='job_00_load_config',
        python_callable=load_config,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_01_load_db_config = PythonOperator(
        task_id='job_01_load_db_config',
        python_callable=load_db_config,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    
    job_02_create_tables = PythonOperator(
        task_id='job_02_create_tables',
        python_callable=create_tables_from_file,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    
    job_03_extract_data = PythonOperator(
        task_id='job_03_extract_data',
        python_callable=extract_data,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_04_transform_data = PythonOperator(
        task_id='job_04_transform_data',
        python_callable=transform_data,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_05_split_data = PythonOperator(
        task_id='job_05_split_data',
        python_callable=split_data,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_06_load_eu = PythonOperator(
        task_id='job_06_load_eu',
        python_callable=load_eu,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_07_load_events = PythonOperator(
        task_id='job_07_load_events',
        python_callable=load_events,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_08_load_lots = PythonOperator(
        task_id='job_08_load_lots',
        python_callable=load_lots,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_09_load_phases = PythonOperator(
        task_id='job_09_load_phases',
        python_callable=load_phases,
        provide_context=True,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Secuencia de ejecución:
    job_00_load_config >> job_01_load_db_config >> job_02_create_tables
    job_02_create_tables >> job_03_extract_data >> job_04_transform_data >> job_05_split_data
    job_05_split_data >> [job_06_load_eu, job_07_load_events, job_08_load_lots, job_09_load_phases]

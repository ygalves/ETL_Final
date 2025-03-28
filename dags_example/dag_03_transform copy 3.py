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

from sqlalchemy import create_engine, text, inspect, MetaData, Table, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------- DBAPI Connection Wrapper ----------------------
class DBAPIConnectionWrapper:
    """
    Wrapper para la conexión DBAPI de SQLAlchemy. Permite exponer el método cursor()
    y otros necesarios para que pandas.to_sql funcione correctamente.
    """
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
    """
    Callback que se ejecuta al finalizar exitosamente una tarea.
    Registra en log el ID de la tarea completada.
    """
    task_id = context.get('task_instance').task_id
    logging.info(f"Tarea {task_id} completada satisfactoriamente.")

def task_failure_callback(context):
    """
    Callback que se ejecuta cuando una tarea falla.
    Registra en log el ID de la tarea y el error ocurrido.
    """
    task_id = context.get('task_instance').task_id
    error = context.get('exception')
    logging.error(f"Tarea {task_id} falló con error: {error}")

# ---------------------- Cargar Configuración y Credenciales ----------------------
def load_config():
    """
    Carga la configuración desde un archivo YAML definido en la variable Airflow "config_path".
    
    Se espera que el YAML tenga la siguiente estructura:
    
    configuration:
      data_path: "/ruta/a/data"
      file_name_input: "Dataset.csv"
      file_extension_input: "html, csv, txt"
      report_retention_days: 7
      url_file: "https://ruta/al/archivo.csv"
      raw_table_name: "etl_dataset"
      transformed_table_name: "etl_transformed"
      eu_table: "eu"
      events_table: "events"
      lots_table: "lots"
      phases_table: "phases"
   
    Retorna un diccionario con la configuración.
    """
    try:
        config_path = Variable.get("config_path", 
                                   default_var=os.path.join(os.path.dirname(__file__), "credentials", "configuration.yaml"))
        with open(config_path, "r") as file:
            config_yaml = yaml.safe_load(file)
        logging.info(f"Configuración cargada desde {config_path}: {config_yaml}")
        cfg = config_yaml.get("configuration", {})
        required_keys = ["data_path", "file_name_input", "file_extension_input",
                         "report_retention_days", "url_file", "raw_table_name",
                         "transformed_table_name", "eu_table", "events_table", "lots_table", "phases_table"]
        for key in required_keys:
            if key not in cfg:
                raise KeyError(f"La clave requerida '{key}' no se encuentra en la configuración.")
        # Se asegura que el directorio definido en data_path exista
        os.makedirs(cfg["data_path"], exist_ok=True)
        return cfg
    except Exception as e:
        logging.error(f"Error al cargar configuración: {e}")
        raise

def load_db_config():
    """
    Carga las credenciales de la base de datos desde un archivo YAML definido en la variable Airflow "db_credential_path".
    
    Se espera que el YAML tenga la siguiente estructura:
    
    database:
      user: "postgres"
      password: "password"
      host: "localhost"
      port: 5432
      name: "ETL_prj"
    
    Retorna un diccionario con las credenciales de conexión.
    """
    try:
        credentials_path = Variable.get("db_credential_path", 
                                        default_var=os.path.join(os.path.dirname(__file__), "credentials", "credentials.yaml"))
        with open(credentials_path, "r") as file:
            cred_yaml = yaml.safe_load(file)
        logging.info(f"Credenciales cargadas desde {credentials_path}: {cred_yaml}")
        db_config = cred_yaml.get("database", {})
        required_db_keys = ["user", "password", "host", "port", "name"]
        for key in required_db_keys:
            if key not in db_config:
                raise KeyError(f"La clave requerida '{key}' no se encuentra en las credenciales de la base de datos.")
        return db_config
    except Exception as e:
        logging.error(f"Error al cargar credenciales de la base de datos: {e}")
        raise

# ---------------------- Creación de Tablas y Constraints ----------------------
def add_constraint_if_not_exists(conn, constraint_name, alter_sql):
    """
    Agrega un constraint a la base de datos si no existe.
    
    Parámetros:
      - conn: Conexión activa a la base de datos.
      - constraint_name: Nombre del constraint.
      - alter_sql: Comando SQL para agregar el constraint.
    """
    result = conn.execute(text("SELECT 1 FROM pg_constraint WHERE conname = :conname"),
                            {"conname": constraint_name}).fetchone()
    if not result:
        conn.execute(text(alter_sql))

def create_constraints(conn):
    """
    Crea los constraints necesarios en las tablas, si no existen.
    """
    add_constraint_if_not_exists(
        conn,
        'fk_events_to_phases',
        "ALTER TABLE phases ADD CONSTRAINT FK_Events_TO_Phases FOREIGN KEY (events_logid) REFERENCES Events (logid)"
    )
    add_constraint_if_not_exists(
        conn,
        'fk_lots_to_phases',
        "ALTER TABLE Phases ADD CONSTRAINT FK_Lots_TO_Phases FOREIGN KEY (lots_logid) REFERENCES Lots (logid)"
    )
    add_constraint_if_not_exists(
        conn,
        'fk_eu_to_phases',
        "ALTER TABLE Phases ADD CONSTRAINT FK_EU_TO_Phases FOREIGN KEY (eu_logid) REFERENCES EU (logid)"
    )

def create_tables_from_file(**kwargs):
    """
    Lee un archivo SQL con el DDL de creación de tablas y lo ejecuta en la base de datos.
    Además, crea los constraints necesarios.

    Separa el contenido del archivo SQL en sentencias individuales (usando el punto y coma como separador)
    y las ejecuta una a una.
    """
    try:
        cfg = load_config()
        db_config = load_db_config()
        sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
        with open(sql_file_path, 'r') as file:
            ddl = file.read()
        logging.info(f"Contenido del archivo SQL:\n{ddl}")

        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Engine URI para crear tablas: {engine_uri}")
        engine = create_engine(engine_uri)

        with engine.connect().execution_options(autocommit=True) as conn:
            # Separa las instrucciones por ';'
            statements = ddl.split(';')
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(text(stmt))
                        logging.info(f"Ejecutada la sentencia: {stmt[:50]}...")
                    except Exception as inner_e:
                        logging.error(f"Error al ejecutar la sentencia: {stmt[:50]}... Error: {inner_e}")
                        raise
            # Después de ejecutar todas las instrucciones, se crean los constraints
            #create_constraints(conn)
        logging.info("Tablas y constraints creados (o ya existentes) correctamente.")
    except Exception as e:
        logging.error(f"Error en create_tables_from_file: {e}")
        raise


# ---------------------- Extracción y Transformación General ----------------------
def extract_data():
    """
    Se conecta a la base de datos y extrae todos los registros de la tabla origen definida en la configuración.
    
    Retorna:
      - Un JSON en formato 'split' que representa el DataFrame extraído.
    """
    try:
        cfg = load_config()
        db_config = load_db_config()
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Cadena del engine: {engine_uri}")
        engine = create_engine(engine_uri)
        query = f"SELECT * FROM {cfg['raw_table_name']};"
        logging.info(f"Ejecutando query: {query}")
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(query, con=conn)
            logging.info(f"Se extrajeron {df.shape[0]} registros de {cfg['raw_table_name']}.")
        finally:
            conn.close()
        return df.to_json(orient='split')
    except Exception as e:
        logging.error(f"Error en extract_data: {e}")
        raise

def transform_data(raw_json):
    """
    Realiza la transformación general del DataFrame extraído:
      - Convierte el JSON a DataFrame.
      - Reemplaza valores no válidos.
      - Convierte columnas numéricas.
      - Limpia cadenas de texto.
      - Renombra la columna 'ID' a 'Lot_ID' (si existe).
      
    Parámetros:
      - raw_json: JSON en formato 'split' del DataFrame original.
      
    Retorna:
      - DataFrame transformado.
    """
    try:
        df = pd.read_json(raw_json, orient='split')
        logging.info(f"Transformando {df.shape[0]} registros.")
        df.replace("#VALUE!", None, inplace=True)
        for col in ['Value', 'Verify']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        for col in [c for c in df.columns if df[c].dtype == object]:
            df[col] = df[col].apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
        if 'ID' in df.columns:
            df.rename(columns={'ID': 'Lot_ID'}, inplace=True)
        logging.info("Transformación general completada.")
        return df
    except Exception as e:
        logging.error(f"Error en transform_data: {e}")
        raise

# ---------------------- Transformaciones Específicas ----------------------
def transform_eu_data(df):
    """
    Extrae y transforma la información correspondiente a unidades de ingeniería (EU).
    
    Parámetros:
      - df: DataFrame general transformado.
      
    Retorna:
      - DataFrame específico para EU.
    """
    df_eu = df['EU'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).to_frame()
    df_eu = df_eu.rename(columns={'EU': 'EU_ID'})
    df_eu['EU_name'] = 'EU_' + df_eu.index.astype(str)
    mapping_desc = {'seg': 'waittime', 'ph': 'acidity', 'kg/l': 'density',
                    'kg': 'weight', 'cp': 'viscosity', 'c': 'temperature', '': 'no assigned'}
    mapping_type = {'seg': 'phase', 'ph': 'control', 'kg/l': 'control',
                    'kg': 'phase', 'cp': 'control', 'c': 'phase', '': 'phase'}
    df_eu['Description'] = df_eu['EU_ID'].map(mapping_desc)
    df_eu['Type'] = df_eu['EU_ID'].map(mapping_type)
    return df_eu

def transform_events_data(df):
    """
    Transforma los datos de fecha y hora para la tabla de eventos.
    Extrae componentes (año, mes, día, etc.) y crea un campo formateado.
    
    Parámetros:
      - df: DataFrame general transformado.
      
    Retorna:
      - DataFrame específico para Events.
    """
    df_events = df['DateTime'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).to_frame()
    df_events["DateTime"] = pd.to_datetime(df_events["DateTime"])
    df_events["Year"] = df_events["DateTime"].dt.year
    df_events["Quarter"] = df_events["DateTime"].dt.quarter
    df_events["Month"] = df_events["DateTime"].dt.month
    df_events["MonthName"] = df_events["DateTime"].dt.month_name()
    df_events["Week"] = df_events["DateTime"].dt.isocalendar().week
    df_events["WeekDay"] = df_events["DateTime"].dt.weekday
    df_events["DayOfYear"] = df_events["DateTime"].dt.dayofyear
    df_events["Day"] = df_events["DateTime"].dt.day
    df_events["DayName"] = df_events["DateTime"].dt.day_name()
    df_events["Hour"] = df_events["DateTime"].dt.hour
    df_events["Minute"] = df_events["DateTime"].dt.minute
    df_events["Second"] = df_events["DateTime"].dt.second
    df_events["Shift_8H"] = np.select(
        [(df_events["Hour"] >= 6) & (df_events["Hour"] < 14),
         (df_events["Hour"] >= 14) & (df_events["Hour"] < 22)],
        [1, 2], default=3)
    df_events["Formatted_Timestamp"] = df_events["DateTime"].dt.strftime('%Y%m%d%H%M%S')
    return df_events

def transform_lots_data(df):
    """
    Transforma los datos para la tabla de lotes:
      - Extrae los productos y les asigna un nombre.
      - Une la información de lotes con la de productos.
      - Agrupa por lote para obtener fechas de inicio y fin, duración y timestamp formateado.
      
    Parámetros:
      - df: DataFrame general transformado.
      
    Retorna:
      - DataFrame específico para Lots.
    """
    df_prods = df['Prod_ID'].drop_duplicates().sort_values(ascending=False).reset_index(drop=True).to_frame()
    
    logging.info(f"df_prods inicial: \n{df_prods.head(10)}")
    
    # Crear una nueva columna 'Producto' con el formato "producto_x" donde x es el índice
    df_prods['Prod_name'] = 'Product_' + df_prods.index.astype(str)
    
    logging.info(f"df_prods asign products: \n{df_prods.head(10)}")
    
    df_sub = df[['Lot_ID', 'DateTime', 'Type', 'Train', 'Prod_ID']].copy().sort_values(by=['DateTime'], ascending=True)
    
    logging.info(f"df_sub get categoricals: \n{df_sub.head(10)}")
    
    df_sub['DateTime'] = pd.to_datetime(df_sub['DateTime'])
    df_sub = df_sub.rename(columns={'Train': 'Train_ID'})
    
    logging.info(f"df_sub rename columns: \n{df_sub.head(10)}")
    
    df_sub = df_sub.merge(df_prods, on='Prod_ID', how='left')
    
    logging.info(f"df_sub merge with df_prods: \n{df_sub.head(10)}")
    
    mapping_train = {'a': 'Train_1', 'b': 'Train_2', 'c': 'Train_3', 'd': 'Train_4',
                     'e': 'Train_5', 'f': 'Train_6', 'g': 'Train_7', 'h': 'Train_8'}
    df_sub['Train'] = df_sub['Train_ID'].map(mapping_train)
    
    logging.info(f"df_sub map train: \n{df_sub.head(10)}")
    
    df_sorted = df_sub.sort_values(by='Lot_ID', ascending=False)
    logging.info(f"df_sorted short \n{df_sorted.head(10)}")
    
    df_lots = df_sub.groupby('Lot_ID', as_index=False).agg(
        Type=('Type', 'first'),
        Train_ID=('Train_ID', 'first'),
        Train=('Train', 'first'),
        Prod_ID=('Prod_ID', 'first'),
        Prod_name=('Prod_name', 'first'),
        First_Date=('DateTime', 'min'),
        Last_Date=('DateTime', 'max')
    )
    
    logging.info(f"df_lots group lots: \n{df_lots.head(10)}")
    
    df_lots = df_lots.sort_values(by='First_Date', ascending=True).reset_index(drop=True)
    df_lots['Duration'] = df_lots['Last_Date'] - df_lots['First_Date']
    df_lots['Duration_sec'] = df['Duration'].dt.total_seconds().astype(int)
    df_lots['Formatted_Timestamp'] = df_lots["First_Date"].dt.strftime('%Y%m%d%H%M%S')
    
    logging.info(f"df_lots last: \n{df_lots.head(10)}")
    return df_lots

def transform_phases_data(df):
    """
    Transforma los datos para la tabla de fases, generando un timestamp formateado.
    
    Parámetros:
      - df: DataFrame general transformado.
      
    Retorna:
      - DataFrame específico para Phases.
    """
    df_phases = df[['DateTime', 'Unit', 'Phase_ID', 'Value', 'EU', 'id']].copy().sort_values(by=['DateTime'], ascending=True)
    df_phases["DateTime"] = pd.to_datetime(df_phases["DateTime"])
    df_phases['Formatted_Timestamp'] = df_phases["DateTime"].dt.strftime('%Y%m%d%H%M%S')
    return df_phases

# ---------------------- Función Genérica de Carga ----------------------
def load_table(table_key, json_data, db_config, dest_table):
    """
    Inserta datos en la tabla destino de la base de datos usando pandas.to_sql.
    
    Parámetros:
      - table_key: Clave identificadora del conjunto (por ejemplo, 'eu', 'events', etc.).
      - json_data: Datos en formato JSON (orient='split') a cargar.
      - db_config: Diccionario con las credenciales de la base de datos.
      - dest_table: Nombre de la tabla destino.
    """
    try:
        df = pd.read_json(io.StringIO(json_data), orient='split')
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"[{table_key}] Engine URI: {engine_uri}")
        engine = create_engine(engine_uri)
        with engine.connect() as conn:
            raw_conn = conn.connection
            wrapped_conn = DBAPIConnectionWrapper(raw_conn)
            logging.info(f"[{table_key}] Primer registro:\n{df.head(1)}")
            logging.info(f"[{table_key}] Shape: {df.shape}")
            df.to_sql(dest_table, con=wrapped_conn, if_exists='append', index=False, method='multi')
            wrapped_conn.commit()
            wrapped_conn.close()
        logging.info(f"Datos cargados en la tabla {dest_table} para el conjunto {table_key}.")
    except Exception as e:
        error_detail = (f"Error al cargar la tabla {dest_table} para el conjunto {table_key}. "
                        f"Engine: {engine_uri if 'engine_uri' in locals() else 'No definido'}, Error: {e}")
        logging.error(error_detail)
        raise

# ---------------------- Funciones de Carga con Transformación Incorporada ----------------------
def load_eu(**kwargs):
    """
    Ejecuta el proceso ETL para la tabla EU:
      - Carga la configuración y las credenciales.
      - Extrae y transforma los datos generales.
      - Aplica la transformación específica para EU.
      - Carga los datos transformados en la tabla destino.
    """
    try:
        db_config = load_db_config()
        raw_json = extract_data()
        df_general = transform_data(raw_json)
        df_eu = transform_eu_data(df_general)
        df_eu.columns = df_eu.columns.str.lower()
        logging.info(f"view df-eu: \n{df_eu.head(10)}")
        json_data = df_eu.to_json(orient='split')
        logging.info(f"view json_data: {json_data}")
        RAW_TABLE_NAME = 'eu'
        
    except Exception as e:
        logging.error(f"Error al cargar credenciales en write_data: {e}")
        raise

    try:
        
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
            f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Cadena del engine: {engine_uri}")
        engine = create_engine(engine_uri)
        insp = inspect(engine)
        if RAW_TABLE_NAME.lower() not in [t.lower() for t in insp.get_table_names()]:
            raise Exception(f"La tabla {RAW_TABLE_NAME} no existe. Créala antes de insertar datos.")
    except Exception as e:
        logging.error(f"Error al inspeccionar la base de datos: {e}")
        raise

    try:
        records = df_eu.to_dict(orient='records')
        metadata = MetaData(bind=engine)
        table = Table(RAW_TABLE_NAME, metadata, autoload_with=engine)
        
        # prevenir duplicados
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(table).values(records)
        # 'eu_id' es la columna que debe ser única.
        stmt = stmt.on_conflict_do_nothing(index_elements=['eu_id'])
        # executar insercion de datos en la tabla
        with engine.begin() as conn:
            conn.execute(stmt)
        logging.info("Datos insertados exitosamente en la tabla mediante insert() sin duplicados.")
    except Exception as e:
        logging.error(f"Error en write_data al insertar datos: {e}")
        raise
 
def load_events(**kwargs):
    """
    Ejecuta el proceso ETL para la tabla Events:
      - Carga las credenciales.
      - Extrae y transforma los datos generales.
      - Aplica la transformación específica para Events.
      - Cambia los encabezados a minúsculas.
      - Verifica que la tabla exista y luego inserta los registros.
    """
    try:
        db_config = load_db_config()
        raw_json = extract_data()
        df_general = transform_data(raw_json)
        df_events = transform_events_data(df_general)
        df_events.columns = df_events.columns.str.lower()
        logging.info(f"view df-events: {df_events.head(10)}")
        #json_data = df_events.to_json(orient='split')
        #logging.info(f"view json_data: {json_data}")
        RAW_TABLE_NAME = 'events'
    except Exception as e:
        logging.error(f"Error al cargar datos para load_events: {e}")
        raise

    try:
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Cadena del engine (events): {engine_uri}")
        engine = create_engine(engine_uri)
        insp = inspect(engine)
        if RAW_TABLE_NAME.lower() not in [t.lower() for t in insp.get_table_names()]:
            raise Exception(f"La tabla {RAW_TABLE_NAME} no existe. Créala antes de insertar datos.")
    except Exception as e:
        logging.error(f"Error al inspeccionar la base de datos en load_events: {e}")
        raise

    try:
        records = df_events.to_dict(orient='records')
        metadata = MetaData(bind=engine)
        table = Table(RAW_TABLE_NAME, metadata, autoload_with=engine)
        # prevenir duplicados
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(table).values(records)
        # 'eu_id' es la columna que debe ser única.
        stmt = stmt.on_conflict_do_nothing(index_elements=['datetime'])
        # executar insercion de datos en la tabla
        with engine.begin() as conn:
            conn.execute(stmt)
        logging.info("Datos insertados exitosamente en la tabla mediante insert() sin duplicados.")
    except Exception as e:
        logging.error(f"Error en write_data al insertar datos: {e}")
        raise
 

def load_lots(**kwargs):
    """
    Ejecuta el proceso ETL para la tabla Lots:
      - Carga las credenciales.
      - Extrae y transforma los datos generales.
      - Aplica la transformación específica para Lots.
      - Cambia los encabezados a minúsculas.
      - Verifica que la tabla exista y luego inserta los registros.
    """
    try:
        db_config = load_db_config()
        raw_json = extract_data()
        df_general = transform_data(raw_json)
        df_lots = transform_lots_data(df_general)
        df_lots.columns = df_lots.columns.str.lower()
        logging.info(f"view df-lots: \n{df_lots.head(10)}")
        #json_data = df_lots.to_json(orient='split')
        #logging.info(f"view json_data: \n{json_data}")
        RAW_TABLE_NAME = 'lots'
    except Exception as e:
        logging.error(f"Error al cargar datos para load_lots: {e}")
        raise

    try:
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Cadena del engine (lots): {engine_uri}")
        engine = create_engine(engine_uri)
        insp = inspect(engine)
        if RAW_TABLE_NAME.lower() not in [t.lower() for t in insp.get_table_names()]:
            raise Exception(f"La tabla {RAW_TABLE_NAME} no existe. Créala antes de insertar datos.")
    except Exception as e:
        logging.error(f"Error al inspeccionar la base de datos en load_lots: {e}")
        raise

    try:
        records = df_lots.to_dict(orient='records')
        metadata = MetaData(bind=engine)
        table = Table(RAW_TABLE_NAME, metadata, autoload_with=engine)
        # prevenir duplicados
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(table).values(records)
        # 'eu_id' es la columna que debe ser única.
        stmt = stmt.on_conflict_do_nothing(index_elements=['lot_id'])
        # executar insercion de datos en la tabla
        with engine.begin() as conn:
            conn.execute(stmt)
        logging.info("Datos insertados exitosamente en la tabla mediante insert() sin duplicados.")
    except Exception as e:
        logging.error(f"Error en write_data al insertar datos: {e}")
        raise
 

def load_phases(**kwargs):
    """
    Ejecuta el proceso ETL para la tabla Phases:
      - Carga las credenciales.
      - Extrae y transforma los datos generales.
      - Aplica la transformación específica para Phases.
      - Cambia los encabezados a minúsculas.
      - Verifica que la tabla exista y luego inserta los registros.
    """
    try:
        db_config = load_db_config()
        raw_json = extract_data()
        df_general = transform_data(raw_json)
        df_phases = transform_phases_data(df_general)
        df_phases.columns = df_phases.columns.str.lower()
        logging.info(f"view df-phases: \n{df_phases.head(10)}")
        #json_data = df_phases.to_json(orient='split')
        #logging.info(f"view json_data: \n{json_data}")
        RAW_TABLE_NAME = 'phases'
    except Exception as e:
        logging.error(f"Error al cargar datos para load_phases: {e}")
        raise

    try:
        engine_uri = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                     f"{db_config['host']}:{db_config['port']}/{str(db_config['name']).lower()}"
        logging.info(f"Cadena del engine (phases): {engine_uri}")
        engine = create_engine(engine_uri)
        insp = inspect(engine)
        if RAW_TABLE_NAME.lower() not in [t.lower() for t in insp.get_table_names()]:
            raise Exception(f"La tabla {RAW_TABLE_NAME} no existe. Créala antes de insertar datos.")
    except Exception as e:
        logging.error(f"Error al inspeccionar la base de datos en load_phases: {e}")
        raise

    try:
        records = df_phases.to_dict(orient='records')
        metadata = MetaData(bind=engine)
        table = Table(RAW_TABLE_NAME, metadata, autoload_with=engine)
        # prevenir duplicados
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = pg_insert(table).values(records)
        # 'eu_id' es la columna que debe ser única.
        stmt = stmt.on_conflict_do_nothing(index_elements=['phase_id'])
        # executar insercion de datos en la tabla
        with engine.begin() as conn:
            conn.execute(stmt)
        logging.info("Datos insertados exitosamente en la tabla mediante insert() sin duplicados.")
    except Exception as e:
        logging.error(f"Error en write_data al insertar datos: {e}")
        raise
 

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
    description="DAG que extrae, transforma y carga datos en tablas (EU, Events, Lots, Phases) usando dos YAML (config y credenciales)",
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'Transform', 'Load', 'Database']
) as dag_etl:

    # Tarea para cargar la configuración (opcional, para validar la carga previa)
    job_load_config = PythonOperator(
        task_id='job_00_load_config',
        python_callable=load_config,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tarea para crear las tablas y constraints en la base de datos
    job_create_tables = PythonOperator(
        task_id='job_01_create_tables',
        python_callable=create_tables_from_file,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Tareas de carga para cada subconjunto de datos
    job_load_eu = PythonOperator(
        task_id='job_02_load_eu',
        python_callable=load_eu,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_load_events = PythonOperator(
        task_id='job_03_load_events',
        python_callable=load_events,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_load_lots = PythonOperator(
        task_id='job_04_load_lots',
        python_callable=load_lots,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    job_load_phases = PythonOperator(
        task_id='job_05_load_phases',
        python_callable=load_phases,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )

    # Secuencia de ejecución:
    # Primero se carga la configuración y se crean las tablas.
    # Luego, se ejecutan en paralelo las tareas de carga de cada conjunto de datos.
    job_load_config >> job_create_tables
    job_create_tables >> [job_load_eu, job_load_events, job_load_lots, job_load_phases]

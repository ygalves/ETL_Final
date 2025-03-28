# %% [markdown]
# <p align="center">
# <img src="https://www.uao.edu.co/wp-content/uploads/2024/12/uao-logo-2-04.webp" width=15%>
# 
# 
# <h2>UNIVERSIDAD AUTÓNOMA DE OCCIDENTE</strong></h2>
# <h3>02/27/2025 CALI - COLOMBIA</strong></h3>
# <h3><strong>MAESTRIA EN INTELIGENCIA ARTIFICIAL Y CIENCIA DE DATOS</strong></h3>
# <h3><strong>ETL (EXTRACT, TRANSFORM AND LOAD)</strong></h3>
# <h3><strong>ENTREGA 1.0. </strong> TRABAJO FINAL - Ejercicio de Extracción de datos usando Dataset de produccion</h3>
# <h3><strong>Profesor:</strong> JAVIER ALEJANDRO VERGARA ZORRILLA</h3>
# <h3><strong>Alumno:</strong></h3>
# 
# <li><font color='lighblue'> 22500214 Yoniliman Galvis Aguirre </font></li>

# %% [markdown]
# #Cargar las librerías
# 
# Primer instalar requirements.txt con el orden indicado de las librerias a instalar en el SO
# 
# ```
# pandas
# sqlalchemy
# psycopg2
# pyyaml
# jupyter
# ```
# 
# <li><b>pandas y sqlalchemy</b> Estas son bibliotecas fundamentales y por eso se deben instalar primero.</li>
# <li><b>psycopg2</b>  es un conector de PostgreSQL para Python y depende de sqlalchemy y debe de instalarse después de sqlalchemy.</li>
# <li><b>pyyaml</b>  es una biblioteca para analizar archivos YAML y puede ser utilizada por otras bibliotecas. es mejor instalarla antes de jupyter.</li>
# <li><b>jupyter</b>  es el entorno de notebook y debe instalarse al final, después de que todas las dependencias estén instaladas.</li>

# %% [markdown]
# ---
# # Verificar Kernel
# Verificamos si el ambiente jupyter esta ejecutando el kernel en el entorno correcto, el resultado de las dos rutas debe coincidir, de lo contrario se debe de cambiar el kernel del jupyter notebook, una opcion es correr el enviroment desde poetry, en la terminal ejecute:
# 
# ```bash
# poetry run jupyter notebook
# ```
# esto abrirá una version web de jupyter, en otro caso cambie el kernel y use los venv disponibles
# 
# Si el notebook esta ejecutando un kernel diferente a la carpeta del proyecto cuando instale librerías se presentarán fallas en la ejecucion del código del notebook

# %%
import subprocess

# Ejecutar el comando de poetry desde Python
result = subprocess.run(['poetry', 'env', 'info', '--path'], capture_output=True, text=True)

# Verificar si el comando se ejecutó correctamente
if result.returncode == 0:
    print("Ruta del entorno virtual:", result.stdout.strip())
else:
    print("Error al ejecutar el comando:", result.stderr)

# %%
import subprocess
import shutil

def get_poetry_env_path():
    try:
        # Ejecutar el comando de poetry desde Python
        result = subprocess.run(['poetry', 'env', 'info', '--path'], capture_output=True, text=True, check=True)
        # Obtener la ruta del entorno virtual
        env_path = result.stdout.strip()
        return env_path
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar el comando de poetry: {e.stderr}")
        return None

def get_active_python_path():
    # Obtener la ruta del ejecutable de Python activo
    python_path = shutil.which("python")
    return python_path

def main():
    poetry_env_path = get_poetry_env_path()
    active_python_path = get_active_python_path()

    if poetry_env_path:
        print(f"El entorno virtual activo de poetry está en: {poetry_env_path}")
    else:
        print("No se pudo obtener la ruta del entorno virtual de poetry.")

    if active_python_path:
        print(f"El entorno virtual activo del kernel en el notebook está en: {active_python_path}")
    else:
        print("No se pudo obtener la ruta del ejecutable de Python activo.")

if __name__ == "__main__":
    main()

# %%
import yaml #manejo de archivos de configuracion .yaml
import psycopg2 #conector de sql, postgresql, mysql, mariaDB
from psycopg2 import sql #driver para postgresql en python
from sqlalchemy import create_engine, text #librería ORM (Object relational Mapper/Mappingtool) para facilitar trabajos con estructura de la DB y los datos
import pandas as pd #permite el manejo, análisis de datos en python
import numpy as np #computacion científica y matemática

# %% [markdown]
# # Funcion de cargar archivo de configuracion de la base de datos
# Esto permite que el código no tenga datos sensibles como claves o direccionamiento a servidores

# %% [markdown]
# Example to create the config.yaml file:
# 
# ```
# database:
#   user: "postgres"
#   password: "password"
#   host: "localhost"
#   port: 5432
#   name: "etl_db"
# ```

# %%
def load_config(file_path="credentials/ETLprj_config.yaml"):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# %% [markdown]
# # Carga y despliegue de CSV
# En este caso el archivo csv se encuentra en la misma carpeta

# %%
# Carga de archivo CSV
csv = pd.read_csv("Dataset/caso etl.csv", header=0,delimiter=',') #espicifico qu etiene encabezados y esta separado por ;

# Mostrar el top(5) del DataFrame
csv.head(10)

# %% [markdown]
# # Cargar parámetros de la DB
# Usando el archivo de config.yam, tomamos los parametros de configuracion, los asigna a etiquetas y los carga en variables internas

# %%
config = load_config()
db_config = config["database"]

# Load credentials
db_user = db_config["user"]
db_password = db_config["password"]
db_host = db_config["host"]
db_port = db_config["port"]
db_name = db_config["name"]

# %% [markdown]
# # Asignando las variables
# se crea el objeto de conexion con la base de datos

# %% [markdown]
# #Posgresql
# 
# para acceder a la base de datos debe ingresar como super user, el ingresará con el usuario del sistema y el password es elmismo del sistema:
# 
# <code> $ sudo su - postgres</code>

# %%
# DB connection
conn = psycopg2.connect(
    dbname="postgres",
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.autocommit = True

# %% [markdown]
# # Crear Base de Datos para el taller 
# Usaos el objeto de conexion y los parámetros dados con anterioridad.

# %% [markdown]
# ![image.png](attachment:image.png)

# %%
db_name = "ETL_prj"
try:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Base de datos '{db_name}' creada exitosamente.")
except psycopg2.errors.DuplicateDatabase:
    print(f"La base de datos '{db_name}' ya existe.")
finally:
    conn.close()

# %% [markdown]
# ![image-3.png](attachment:image-3.png)

# %% [markdown]
# # Crear la tabla en la base de datos
# Primero se verifican las columnas y se intenta saber que tipo son

# %%
# Mostrar el header del DataFrame
csv.head(1)

# %%
print(csv.info())

# %%
import seaborn as sns
from traitlets.traitlets import Long
import matplotlib.pyplot as plt

# Showing Quantity Product type made by train
g = sns.FacetGrid(csv, col="Type",  hue="Prod_ID", margin_titles=True, height = 3)
g.map(sns.histplot, "Train")
g.figure.subplots_adjust(wspace=.02, hspace=.02)
# Show plot
plt.show()

# %%
# Explode Shoes slice
my_explode = [0.1, 0.2, 0, 0.3] #explode=my_explode

# color palette
font_color = '#525252'
colors = ['#0038E2', '#008DB8', '#00E28E', '#191970',]

# Create subplots and a pie chart
fig, ax = plt.subplots(figsize=(10, 7), facecolor='#e8f4f0')

# Create pie chart
ax = csv.groupby('Type').count()['Prod_ID'].plot.pie(y=csv['Type'],
                                                       title="by Product Type",
                                                       figsize=(3.5, 3.5),
                                                       fontsize=9,
                                                       wedgeprops=dict(width=.5), # For donuts
                                                       colors=colors,
                                                       textprops={'color':font_color},
                                                       autopct='%1.1f%%',
                                                       shadow=False,

                                                       )


# %%
# Get data by Train Column and count of products by train
Grp_Train_Prod = csv.groupby('Train').count()['Prod_ID']

# Get data by type column and count products by type
Grp_Type_Prod = csv.groupby('Type').count()['Prod_ID']

# Get data by train Column and count type products by train
Grp_Train_Type = csv.groupby('Train').count()['Type']

# Get data by product Column and count type phase by product
Grp_Phase_Type = csv.groupby('Prod_ID').count()['Phase_ID']

# Mostrar los resultados
print('Group Products by Train \n {} \n '.format(Grp_Train_Prod.sort_values(ascending=True)))
print('Group Products by Type \n {} \n '.format(Grp_Type_Prod.sort_values(ascending=True)))
print('Group Type by Train \n {} \n '.format(Grp_Train_Type.sort_values(ascending=True)))
print('Group phase by Product \n {} \n '.format(Grp_Phase_Type.sort_values(ascending=True)))

# %% [markdown]
# # Crear Base de Datos
# Se define el motor de la base de datos que es una cadena de texto que contiene los parametros necesarios para la conexion
# luego usando este motor se envia un tsql al motor de DB para crear la tabla donde vamos a llevar los datos en el csv, se deben de crear las columnas con nombres iguales al header del dataframe

# %%
# crear conexión a la base de datos
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# %%

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS Dataset (
            id SERIAL PRIMARY KEY,
            "DateTime" Timestamptz,
            "ID" VARCHAR(250),
            "Prod_ID" VARCHAR(100),
            "Type" VARCHAR(10),
            "Train" VARCHAR(10),
            "Unit" VARCHAR(10),
            "Phase_ID" VARCHAR(100),
            "EU" VARCHAR(10),
            "Value" Float,
            "Verify" smallint
        );
    """))
    conn.commit()  # Asegúrate de confirmar los cambios
    print("Tabla 'Dataset' creada exitosamente en PostgreSQL.")

# %% [markdown]
# ![image.png](attachment:image.png)

# %% [markdown]
# # Inserción de datos
# Insertamos los datos del dataframe en la tabla creada y usando los datos del engine, si los datos ya existen los va a adicionar y no va ha tratar de escribir el indice ya que la tabla tiene un indice que incrementa de forma automática al crear registros nuevos

# %% [markdown]
# ![image.png](attachment:image.png)

# %%
# Insertar datos en la tabla usando DataFrame a la tabla en PostgreSQL

try:
    csv.to_sql('dataset', engine, if_exists='append', index=False)
    print("Datos insertados exitosamente.")
except SQLAlchemyError as e:
    print(f"Error al insertar los datos: {e}")

# %% [markdown]
# ![image.png](attachment:image.png)

# %% [markdown]
# # Leer y Cargar Datos
# Leemos los datos y los mostramos

# %%
# Leer datos de la tabla
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM dataset LIMIT 10;"))
    rows = result.fetchall()

# Mostrar los resultados
print("Datos en 'tabla_etl':")
for row in rows:
    print(row)

# %% [markdown]
# # Cargamos los datos a un Dataframe haciendo una consulta simple

# %%
with engine.connect() as conn:
    df = pd.read_sql("SELECT * FROM dataset", conn)
df.head(10)



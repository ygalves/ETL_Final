import yaml #manejo de archivos de configuracion .yaml
import psycopg2 #conector de sql, postgresql, mysql, mariaDB
from psycopg2 import sql #driver para postgresql en python
from sqlalchemy import create_engine, text #librería ORM (Object relational Mapper/Mappingtool) para facilitar trabajos con estructura de la DB y los datos
import pandas as pd #permite el manejo, análisis de datos en python
import numpy as np #computacion científica y matemática

# Carga de archivo CSV
csv = pd.read_csv("Dataset/caso etl.csv", header=0,delimiter=',') #espicifico qu etiene encabezados y esta separado por ;

print(csv.head(10))
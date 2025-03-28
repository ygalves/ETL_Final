"""
DAG: test1

Descripción:
    Este DAG de ejemplo define tres tareas:
      - job0: Tarea de Python que verifica la configuración del DAG run. Si el parámetro "commit" es "1", 
              lanza una excepción para forzar el fallo.
      - print_date (job1): Tarea Bash que imprime la fecha actual.
      - job2: Tarea de Python que recupera el valor devuelto por job0 mediante XCom y lo registra en los logs.

Requisitos:
    - Airflow debe estar correctamente configurado.
    - Las dependencias necesarias (Airflow, etc.) deben estar instaladas.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Argumentos predeterminados para el DAG
dag_args = {
    'depends_on_past': False,           # No depende de ejecuciones previas
    'email': ["yoniliman.galves@uao.edu.co"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Parámetros opcionales (descomentar si se requieren):
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'start_date': datetime(2025, 3, 2),
    # 'end_date': datetime(2025, 5, 2),
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
}

# Definición del DAG utilizando el contexto "with" (mejor práctica)
with DAG(
    'test1',
    description="Mi primer DAG",
    default_args=dag_args,
    schedule_interval=timedelta(days=1),  # El DAG se ejecutará diariamente
    start_date=datetime(2025, 1, 1),       # Fecha de inicio
    catchup=False,                        # No ejecutar tareas retroactivas
    tags=['example'],
    params={"commit": "000000"}           # Parámetro por defecto para el DAG
) as dag:

    def job0_func(**kwargs):
        """
        Tarea job0: Verifica la configuración del DAG run y decide si fallar la tarea.

        Revisa si en la configuración del DAG run existe el parámetro "commit" con valor "1". 
        En ese caso, lanza una excepción para forzar el fallo de la tarea.

        Args:
            **kwargs: Argumentos adicionales proporcionados por Airflow, que incluyen 'dag_run'.

        Returns:
            dict: Un diccionario indicando que la tarea se completó correctamente.

        Raises:
            AirflowFailException: Si el parámetro "commit" es igual a "1".
        """
        # Obtener la configuración del DAG run
        conf = kwargs['dag_run'].conf
        if "commit" in conf and conf["commit"] == "1":
            raise AirflowFailException("Hoy se jodió todo, no desplegamos nada")
        return {"ok": 1}

    def job2_func(**kwargs):
        """
        Tarea job2: Recupera el valor XCom de la tarea job0 y lo registra en los logs.

        Utiliza el método xcom_pull para obtener el valor devuelto por la tarea 'job0'
        y lo registra.

        Args:
            **kwargs: Argumentos adicionales proporcionados por Airflow, que incluyen 'ti' (Task Instance).

        Returns:
            dict: Un diccionario indicando que la tarea se completó correctamente.
        """
        # Recuperar el valor devuelto por job0 utilizando XCom
        xcom_value = kwargs['ti'].xcom_pull(task_ids='job0')
        logging.info("Hello world")
        logging.info("Valor obtenido desde XCom de job0: %s", xcom_value)
        return {"ok": 2}

    # Definición de las tareas
    job0 = PythonOperator(
        task_id='job0',
        python_callable=job0_func,
        provide_context=True  # Permite el paso de contexto (kwargs) a la función
    )

    job1 = BashOperator(
        task_id='print_date',
        bash_command='echo "La fecha es $(date)"'
    )

    job2 = PythonOperator(
        task_id='job2',
        python_callable=job2_func,
        provide_context=True
    )

    # Establecer dependencias:
    # job0 se ejecuta antes de job1 y job2
    job0 >> [job1, job2]

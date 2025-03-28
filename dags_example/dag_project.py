from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
}

# Step 3: Creating DAG Object
dag = DAG(dag_id='DAG-project',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

# Step 4: Creating task
# Creating first task
extract = DummyOperator(task_id = 'extract', dag = dag)
# Creating second task 
transform = DummyOperator(task_id = 'transform', dag = dag)

load = DummyOperator(task_id = 'load', dag = dag)

paralell = DummyOperator(task_id = 'paralell', dag = dag)

 # Step 5: Setting up dependencies 
extract >> transform >> load
paralell >> load
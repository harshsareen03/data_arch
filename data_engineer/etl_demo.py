from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    pass

def transform():
    pass

def load():
    pass

dag=DAG('etl_demo',start_date=datetime(2025,9,16),schedule_interval='@daily')


t1=PythonOperator(task_id='extract',python_callable=extract,dag=dag)
t2=PythonOperator(task_id='transform',python_callable=transform,dag=dag)
t3=PythonOperator(task_id='load',python_callable=load,dag=dag)

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1),
    'retries': 2,
}

dag = DAG('tomcat_service_control', default_args=default_args, schedule_interval='@daily')

start_task = BashOperator(
    task_id='start_tomcat',
    bash_command='sudo systemctl start tomcat1',
    dag=dag,
)

stop_task = BashOperator(
    task_id='stop_tomcat',
    bash_command='sudo systemctl stop tomcat1',
    dag=dag,
)

start_task >> stop_task

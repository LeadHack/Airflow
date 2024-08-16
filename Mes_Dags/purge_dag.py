
# DAG de Purge
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'purge_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

start = DummyOperator(task_id='start', dag=dag)

purge_logs = BashOperator(
    task_id='purge_logs',
    bash_command=(
        'dir_to_purge=$(echo "/root/airflow/Log_test" | sed "s:/*$::"); '  # Supprimer les slashes finaux
        'if [ -d "$dir_to_purge" ]; then '  # Verifier si le repertoire existe
        'Nbr_File_A_purger=$(find "$dir_to_purge"/ -type f -name "*.log" | wc -l); '
        'echo "Nombre de fichier A supprimer est : $Nbr_File_A_purger" ';
        'rm "$dir_to_purge"/*.log; '  # Supprimer les fichiers .log si le repertoire existe
        'echo "Purge effectuee" ; '  # Supprimer les fichiers .log si le repertoire existe
        'else echo "Repertoire non trouve : $dir_to_purge"; fi'  # Afficher un message si le repertoire n'existe pas
    ),
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >>  purge_logs >> end

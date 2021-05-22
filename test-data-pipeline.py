# [START import_module]
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
# [END import_module]

# [START default_args]
default_args = {
    'owner': 'Alex Augusto C.',
    'depends_on_past': False,
    'email': ['alexno999@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'test-data-pipeline',
    default_args=default_args,
    start_date=datetime(2021, 5, 22),
    schedule_interval='@weekly',
    tags=['test', 'development', 'bash'])
# [END instantiate_dag]

# [START basic_task]
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag)
# [END basic_task]

# [START task_sequence]
t1 >> [t2]
# [END task_sequence]

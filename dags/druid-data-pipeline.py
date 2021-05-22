# [START import_module]
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
# [END import_module]

# [START default_args]
default_args = {
    'owner': 'adilson cesar silva',
    'depends_on_past': False,
    'email': ['adilson.silva@owshq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'druid-test-pipeline',
    default_args=default_args,
    start_date=datetime(2021, 3, 18),
    schedule_interval='@weekly',
    tags=['druid', 'test', 'development', 'bash'])
# [END instantiate_dag]

# [START basic_task]
t1 = BashOperator(
    task_id='Get_Status_of_Druid_Task_acks0',
    bash_command='curl http://druid-router.datastore.svc.cluster.local:8888/druid/indexer/v1/supervisor/producer-test-strimzi-dev-acks-0/status',
    dag=dag)

t2 = BashOperator(
    task_id='Only_a_sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag)
# [END basic_task]

# [START task_sequence]
t1 >> [t2]
# [END task_sequence]

# [START pre_requisites]
# create connectivity to kubernetes [minio] ~ in-cluster configuration
# files yelp_academic_dataset_business_2018.json and yelp_academic_dataset_business_2018.json inside of processing/pr-elt-business
# spark operator deployed on processing namespace
# [END pre_requisites]

# [START import_module]
from os import getenv
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
# [END import_module]

# [START env_variables]
PROCESSING_ZONE = getenv("PROCESSING_ZONE", "processing")
# [END env_variables]

# [START default_args]
default_args = {
    'owner': 'luan moreno m. maciel',
    'start_date': datetime(2021, 3, 24),
    'depends_on_past': False,
    'email': ['luan.moreno@owshq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    's3-pr-elt-business-spark-operator',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['development', 's3', 'sensor', 'minio', 'spark', 'operator', 'k8s'])
# [END instantiate_dag]

# [START set_tasks]
# verify if new data has arrived on processing bucket
# connecting to minio to check (sensor)
verify_file_existence_processing = S3KeySensor(
    task_id='verify_file_existence_processing',
    bucket_name=PROCESSING_ZONE,
    bucket_key='pr-elt-business/*.json',
    wildcard_match=True,
    timeout=18 * 60 * 60,
    poke_interval=120,
    aws_conn_id='minio',
    dag=dag)

# use spark-on-k8s to operate against the data
# containerized spark application
# yaml definition to trigger process
pr_elt_business_spark_operator = SparkKubernetesOperator(
    task_id='pr_elt_business_spark_operator',
    namespace='processing',
    application_file='pr-elt-business.yaml',
    kubernetes_conn_id='minikube',
    do_xcom_push=True,
    dag=dag)

# monitor spark application
# using sensor to determine the outcome of the task
monitor_spark_app = SparkKubernetesSensor(
    task_id='monitor_spark_app',
    namespace="processing",
    application_name="{{ task_instance.xcom_pull(task_ids='pr_elt_business_spark_operator')['metadata']['name'] }}",
    kubernetes_conn_id="minikube",
    dag=dag)

# delete file from processed zone
delete_s3_file_processed_zone = S3DeleteObjectsOperator(
    task_id='delete_s3_file_processed_zone',
    bucket=PROCESSING_ZONE,
    keys='pr-elt-business/',
    aws_conn_id='minio',
    dag=dag)
# [END set_tasks]

# [START task_sequence]
verify_file_existence_processing >> pr_elt_business_spark_operator >> monitor_spark_app >> delete_s3_file_processed_zone
# [END task_sequence]

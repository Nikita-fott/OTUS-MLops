from airflow import DAG
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {"owner": "airflow"}

dag = DAG(
    dag_id='testing_ssh',
    default_args=args,
    description='testing',
    start_date=datetime(2023, 12, 8),
    schedule_interval='*/5 * * * *',
    catchup=False
)

test_mkdir = SSHOperator(
    task_id='test_mkdir',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -mkdir /otus_test',
    dag=dag
)
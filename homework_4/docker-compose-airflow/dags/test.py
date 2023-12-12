from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="1test_ssh_new",
    default_args=args,
    description='testing',
    start_date=datetime(2023, 12, 4),
    schedule_interval='*/5 * * * *',
    catchup=False
)


test_mkdir = SSHOperator(
    task_id='test_hdfs',
    ssh_conn_id='mlops_ssh',
    command='hdfs dfs -mkdir /otus_test',
    dag=dag
)
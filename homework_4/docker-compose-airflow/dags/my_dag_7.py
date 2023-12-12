from airflow import DAG
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


args = {"owner": "airflow"}

dag = DAG(
    dag_id="test_ssh_hook",
    default_args=args,
    description='testing',
    start_date=datetime(2023, 12, 4),
    schedule_interval='*/5 * * * *',
    catchup=False
)

t4 = SSHOperator(
	task_id='SSHOperator',
	ssh_conn_id='mlops_ssh',
	command='ls la',
    dag=dag
)
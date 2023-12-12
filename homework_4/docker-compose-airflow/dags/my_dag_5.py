from airflow import DAG
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator


args = {"owner": "airflow"}

dag = DAG(
    dag_id="test_ssh_new",
    default_args=args,
    description='testing',
    start_date=datetime(2023, 12, 4),
    schedule_interval='*/5 * * * *',
    catchup=False,
)

t4 = SSHOperator(
	task_id='SSHOperator-test',
	ssh_conn_id='mlops-ssh',
	command='mkdir /home/ubuntu/test_ssh',
    dag=dag
)
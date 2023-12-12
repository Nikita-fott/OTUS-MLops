from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator


sample_command = """
    python3 /home/ubuntu/validate_data.py
"""


args = {"owner": "airflow"}


dag_validation= DAG(
    dag_id='validate_data',
    default_args=args,
    description='run puthon script for validate dataset',
    # schedule_interval='@once',
    schedule_interval='*/3 * * * *',
    start_date=datetime(2023, 12, 8),
    catchup=False
)

print("upload df from s3 to local")
t1 = SSHOperator(
    task_id='task_validate_data',
    ssh_conn_id='mlops_ssh',
    command=sample_command,
    cmd_timeout=None,
    dag=dag_validation
)
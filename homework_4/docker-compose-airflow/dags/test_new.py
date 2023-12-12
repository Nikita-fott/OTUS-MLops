from airflow import DAG
from datetime import timedelta, datetime
from airflow.decorators import task
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 8)
}

with DAG('xcom_test_ssh_new', schedule_interval='*/5 * * * *', default_args=args):
    @task(task_id='test_hdfs')
    def push():
        return "hdfs dfs -mkdir /test_mkdir"

    xcom_pull_value = "{{ task_instance.xcom_pull(task_ids='test_hdfs') }}"
    test_hdfs_1 = SSHOperator(
        task_id='test_hdfs_1',
        ssh_conn_id='mlops_ssh',
        command=f"""'{xcom_pull_value}' | base64 --decode"""
    )

    push() >> test_hdfs_1
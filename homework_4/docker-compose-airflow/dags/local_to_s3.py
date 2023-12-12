from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {"owner": "airflow"}

sample_command = """
    hadoop distcp \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.endpoint=storage.yandexcloud.net \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.acces.key=YCAJEaeqhAnxudfLGD71dB2XT \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.secret.key=YCNuIKCL2fbt_iGCixpO8cxrPp8Sr1qgK3yPsv7r \
        -update -skipcrccheck -numListstatusThreads 10 \
        hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net/data_fraud/df_validation_2019-09-21.parquet \
        s3a://mlops-validation-dataset-29102023/parquet-29102023
"""

dag_hadoop_del= DAG(
    dag_id='dag_from_hdfs_to_s3',
    default_args=args,
    description='use service account to copy from hdfs to s3',
    # schedule_interval='@once',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 12, 8),
    catchup=False
)

args = {"owner": "airflow"}


to_s3_from_hdfs = SSHOperator(
    task_id='to_s3_from_hdfs',
    ssh_conn_id='mlops_ssh',
    command=sample_command,
    cmd_timeout=None,
    dag=dag_hadoop_del
)

delete_dir = SSHOperator(
    task_id='delete_dir',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -rm -r /data_fraud',
    cmd_timeout=None,
    dag=dag_hadoop_del
)

to_s3_from_hdfs>>delete_dir
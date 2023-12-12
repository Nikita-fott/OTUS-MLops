from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator


sample_command = """
    sudo -u hdfs hadoop distcp \
        -D fs.s3a.bucket.mlops-backet-16102023.endpoint=storage.yandexcloud.net \
        -D fs.s3a.bucket.mlops-backet-16102023.acces.key=YCAJEaeqhAnxudfLGD71dB2XT \
        -D fs.s3a.bucket.mlops-backet-16102023.secret.key=YCNuIKCL2fbt_iGCixpO8cxrPp8Sr1qgK3yPsv7r \
        -update -skipcrccheck -numListstatusThreads 10 \
        s3a://mlops-backet-16102023/2019-09-21.txt \
    hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net/data_fraud
"""


args = {"owner": "airflow"}


dag_hadoop_create= DAG(
    dag_id='dag_from_s3_to_hdfs',
    default_args=args,
    description='use service account to copy from S3 to hdfs local',
    # schedule_interval='@once',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 12, 8),
    catchup=False
)

creat_dir = SSHOperator(
    task_id='creat_dir',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -mkdir /data_fraud',
    dag=dag_hadoop_create
)

give_permissions = SSHOperator(
    task_id='give_permissions',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -chmod -R 777 /data_fraud',
    dag=dag_hadoop_create
)

change_user = SSHOperator(
    task_id='change_user',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -chown ubuntu /data_fraud',
    dag=dag_hadoop_create
)

upload_df_from_s3 = SSHOperator(
    task_id='task_upload_df_from_s3',
    ssh_conn_id='mlops_ssh',
    command=sample_command,
    cmd_timeout=None,
    dag=dag_hadoop_create
)

creat_dir>>give_permissions>>change_user>>upload_df_from_s3

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator


command_for_copy_to_hdfs = """
    sudo -u hdfs hadoop distcp \
        -D fs.s3a.bucket.mlops-backet-16102023.endpoint=storage.yandexcloud.net \
        -D fs.s3a.bucket.mlops-backet-16102023.acces.key=YCAJEaeqhAnxudfLGD71dB2XT \
        -D fs.s3a.bucket.mlops-backet-16102023.secret.key=YCNuIKCL2fbt_iGCixpO8cxrPp8Sr1qgK3yPsv7r \
        -update -skipcrccheck -numListstatusThreads 10 \
        s3a://mlops-backet-16102023/2019-09-21.txt \
    hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net/data_fraud
"""


args = {"owner": "airflow"}


dag_saprk_validation= DAG(
    dag_id='dag_data_preparation_pipeline',
    default_args=args,
    description='use service account to copy from S3 to hdfs local',
    # schedule_interval='@once',
    schedule_interval='*/20 * * * *',
    start_date=datetime(2023, 12, 8),
    catchup=False
)

creat_dir = SSHOperator(
    task_id='creat_dir',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -mkdir /data_fraud',
    cmd_timeout=None,
    dag=dag_saprk_validation
)

give_permissions = SSHOperator(
    task_id='give_permissions',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -chmod -R 777 /data_fraud',
    cmd_timeout=None,
    dag=dag_saprk_validation
)

change_user = SSHOperator(
    task_id='change_user',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -chown ubuntu /data_fraud',
    cmd_timeout=None,
    dag=dag_saprk_validation
)

upload_df_from_s3 = SSHOperator(
    task_id='task_upload_df_from_s3',
    ssh_conn_id='mlops_ssh',
    command=command_for_copy_to_hdfs,
    cmd_timeout=None,
    dag=dag_saprk_validation
)


run_script_validation = """
    python3 /home/ubuntu/validate_data.py
"""

validate_data = SSHOperator(
    task_id='validate_data',
    ssh_conn_id='mlops_ssh',
    command=run_script_validation,
    cmd_timeout=None,
    dag=dag_saprk_validation
)

command_for_copy_to_s3 = """
    hadoop distcp \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.endpoint=storage.yandexcloud.net \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.acces.key=YCAJEaeqhAnxudfLGD71dB2XT \
        -D fs.s3a.bucket.mlops-validation-dataset-29102023.secret.key=YCNuIKCL2fbt_iGCixpO8cxrPp8Sr1qgK3yPsv7r \
        -update -skipcrccheck -numListstatusThreads 10 \
        hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net/data_fraud/df_validation_2019-09-21.parquet \
        s3a://mlops-validation-dataset-29102023/parquet-29102023
"""

to_s3_from_hdfs = SSHOperator(
    task_id='to_s3_from_hdfs',
    ssh_conn_id='mlops_ssh',
    command=command_for_copy_to_s3,
    cmd_timeout=None,
    dag=dag_saprk_validation
)

delete_dir = SSHOperator(
    task_id='delete_dir',
    ssh_conn_id='mlops_ssh',
    command='sudo -u hdfs hadoop fs -rm -r /data_fraud',
    cmd_timeout=None,
    dag=dag_saprk_validation
)

creat_dir >> give_permissions >> change_user >> upload_df_from_s3 >> validate_data >> to_s3_from_hdfs >> delete_dir
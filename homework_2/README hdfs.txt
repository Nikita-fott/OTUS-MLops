получил целевой сервер HDFS


команда копирования файлов из бакета:
ubuntu@rc1a-dataproc-m-en53zzoqma6sde0e:~$ hadoop distcp -D fs.s3a.bucket.mlops-backet-16102023.endpoint=storage.yandexcloud.net -D fs.s3a.bucket.mlops-backet-16102023.acces.key=**** -D fs.s3a.bucket.mlops-backet-16102023.secret.key=**** -update -skipcrccheck -numListstatusThreads 10 s3a://mlops-backet-16102023/ hdfs://rc1a-data
proc-m-en53zzoqma6sde0e.mdb.yandexcloud.net/user/root/datasets/set01/


исполнение команды


содержимое HDFS
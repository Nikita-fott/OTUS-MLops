import os
import sys
import numpy as np

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import findspark
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
import re
from pyspark.sql.functions import col, asc, desc, isnan, when, trim
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import udf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import PipelineModel
import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime


def main():
    spark = (
        SparkSession
        .builder
        .appName("mlops")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.instances", "3")
        .config("spark.default.parallelism", "24")
        .getOrCreate()
    )

    df = spark.read.parquet("hdfs://rc1a-dataproc-m-dt27mwn7ayzjtxu7.mdb.yandexcloud.net:8020/data_fraud/data_2022-06-07.parquet")

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    TRACKING_SERVER_HOST = "158.160.40.25"
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:8000")

    experiment = mlflow.set_experiment("nikita-fott-test-1")
    experiment_id = experiment.experiment_id

    run_name = 'My run name' + 'TEST_LogReg' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        model = PipelineModel.load("pipelineModel_TV")

        run_id = mlflow.active_run().info.run_id

        predicted = model.transform(df)

        predicted_metrics = udfModelEvaluator(predicted, "tx_fraud")

        mlflow.log_metric("accuracy", predicted_metrics['A'])
        mlflow.log_metric("precision", predicted_metrics['P'])
        mlflow.log_metric("recall", predicted_metrics['R'])
        mlflow.log_metric("Fmeasure", predicted_metrics['F1'])
        mlflow.log_metric("Fmeasure", predicted_metrics['ConfusionMatrix'])
        mlflow.log_metric("TP", predicted_metrics['TP'])
        mlflow.log_metric("TN", predicted_metrics['TN'])
        mlflow.log_metric("FP", predicted_metrics['FP'])
        mlflow.log_metric("FN", predicted_metrics['FN'])

        mlflow.spark.save_model(model, "LrModelSaved")
        mlflow.spark.log_model(model, "LrModelLogs")

    spark.stop()

    print("Done")
if __name__ == "__main__":
    main()
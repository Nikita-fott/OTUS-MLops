import os
import sys
import logging

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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline

import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


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

    logger.info("Read parquet")
    df = spark.read.parquet("hdfs://rc1a-dataproc-m-dt27mwn7ayzjtxu7.mdb.yandexcloud.net:8020/data_fraud/data_2022-06-07.parquet")

    logger.info("Checking balance")
    df_1 = df.filter(df["tx_fraud"] == 1)
    df_0 = df.filter(df["tx_fraud"] == 0)

    df_1count = df_1.count()
    df_0count = df_0.count()

    logger.info("Balancing samples by tx_fraud")
    df1Over = df_1 \
        .withColumn("dummy",
                    f.explode(
                        f.array(*[f.lit(x)
                                  for x in range(int(df_0count / df_1count))]))) \
        .drop("dummy")

    logger.info("Union all to data")
    data = df_0.unionAll(df1Over)

    logger.info("StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler")
    data_indexer = StringIndexer(inputCols=["percent_fraud_on_terminal_udf", "tx_fraud_scenario"], outputCols=["PercFraudIndex", "FraudScenarioIndex"])
    data_encoder = OneHotEncoder(inputCols=["PercFraudIndex", "FraudScenarioIndex"], outputCols=["PercFraudEncoded", "FraudScenarioEncoded"])

    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount"]
    cat_cols = ["PercFraudEncoded", "FraudScenarioEncoded"]
    featureColumns = numeric_cols + cat_cols

    assembler = VectorAssembler() \
        .setInputCols(featureColumns) \
        .setOutputCol("features") \
        .setHandleInvalid("skip")

    scaler = MinMaxScaler() \
        .setInputCol("features") \
        .setOutputCol("scaledFeatures")

    logger.info("Pipeline to data")

    scaled = Pipeline(stages=[
        data_indexer,
        data_encoder,
        assembler,
        scaler,
    ]).fit(data).transform(data)

    logger.info("Training and test samples, 80/20")
    tt = scaled.randomSplit([0.8, 0.2])
    training = tt[0]
    test = tt[1]

    logger.info("Mlflow is starting")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    mlflow.set_tracking_uri("http://51.250.79.10:8000")

    experiment = mlflow.set_experiment("nikita-fott-test-1")
    experiment_id = experiment.experiment_id

    run_name = 'My run name' + 'TEST_LogReg' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        setRegParam = 0.2
        setElasticNetParam = 0.8
        
        logger.info("LogisticRegression is starting")
        lr = LogisticRegression() \
            .setMaxIter(1) \
            .setRegParam(setRegParam) \
            .setElasticNetParam(setElasticNetParam) \
            .setFamily("binomial") \
            .setFeaturesCol("scaledFeatures") \
            .setLabelCol("tx_fraud")

        lrModel = lr.fit(training)

        run_id = mlflow.active_run().info.run_id

        mlflow.log_param('optimal_regParam', setRegParam)
        mlflow.log_param('optimal_elasticNetParam', setElasticNetParam)
        
        logger.info("Training is starting")
        trainingSummary = lrModel.summary
        accuracy_trainingSummary = trainingSummary.accuracy
        areaUnderROC_trainingSummary = trainingSummary.areaUnderROC

        mlflow.log_metric("accuracy_trainingSummary", accuracy_trainingSummary)
        mlflow.log_metric("areaUnderROC_trainingSummary", areaUnderROC_trainingSummary)
        
        logger.info("Predicted is starting")
        predicted = lrModel.transform(test)
        
        logger.info("tp, tn, fp, fn")
        tp = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()
        tn = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 0)).count()
        fp = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()
        fn = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()
        
        logger.info("accuracy, precision, recall, Fmeasure")
        accuracy = (tp + tn) / (tp + tn + fp + fn)
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        Fmeasure = 2 * recall * precision / (recall + precision)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("Fmeasure", Fmeasure)

        mlflow.spark.save_model(lrModel, "LrModelSaved")
        mlflow.spark.log_model(lrModel, "LrModelLogs")

    spark.stop()

    print("Done")
if __name__ == "__main__":
    main()

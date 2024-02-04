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
from pyspark.sql.functions import col, asc, desc, isnan, when, trim, udf
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import Pipeline
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.stats  import norm, ttest_ind
from scipy import stats
import matplotlib as plot
import seaborn as sns

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

dict_report = {}

bootstrap_iterations = 10

def udfModelEvaluator(dfPredictions, labelColumn):
    colSelect = dfPredictions.select([F.col('prediction').cast(DoubleType()), F.col(labelColumn).cast(DoubleType()).alias('label')])

    metrics = MulticlassMetrics(colSelect.rdd)

    mAccuracy = metrics.accuracy
    mPrecision = metrics.precision(1)
    mRecall = metrics.recall(1)
    mF1 = metrics.fMeasure(1.0, 1.0)

    mMatrix = metrics.confusionMatrix().toArray().astype(int)

    mTP = metrics.confusionMatrix().toArray()[1][1]
    mTN = metrics.confusionMatrix().toArray()[0][0]
    mFP = metrics.confusionMatrix().toArray()[0][1]
    mFN = metrics.confusionMatrix().toArray()[1][0]

    mResults = {'Accuracy': mAccuracy, 'Precision': mPrecision, 'Recall': mRecall,
                'F1': mF1, 'ConfusionMatrix': mMatrix, 'TP': mTP, 'TN': mTN, 'FP': mFP, 'FN': mFN}

    return mResults

def main():
    logger.info("Read parquet") #чтение провалидированного датасета
    df = spark.read.parquet("hdfs://rc1a-dataproc-m-dt27mwn7ayzjtxu7.mdb.yandexcloud.net:8020/data_fraud/data_2022-06-07.parquet")

    logger.info("Balancing samples by tx_fraud") #балансировка данных, так как tx_fraud=1 в разы меньше tx_fraud=0
    df_1 = df.filter(df["tx_fraud"] == 1)
    df_0 = df.filter(df["tx_fraud"] == 0)

    df_1count = df_1.count()
    df_0count = df_0.count()

    df1Over = df_1 \
        .withColumn("dummy",
                    f.explode(
                        f.array(*[f.lit(x)
                                  for x in range(int(df_0count / df_1count))]))) \
        .drop("dummy") #реплецировали tx_fraud = 1 df_0count / df_1count раз

    logger.info("Union all to data")
    data = df_0.unionAll(df1Over) #объединили 0 и реплицированный ДФ с 1 в один общий

    logger.info("StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler")
    data_indexer = StringIndexer(inputCols=["percent_fraud_on_terminal_udf", "tx_fraud_scenario"], outputCols=["PercFraudIndex", "FraudScenarioIndex"]) #индексируем строковые колонки
    data_encoder = OneHotEncoder(inputCols=["PercFraudIndex", "FraudScenarioIndex"], outputCols=["PercFraudEncoded", "FraudScenarioEncoded"]) #кодируем категориальные признаки

    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount"] #объявили числовые столбцы
    cat_cols = ["PercFraudEncoded", "FraudScenarioEncoded"] #категориальные столбцы
    featureColumns = numeric_cols + cat_cols #объединили в фичи

    assembler = VectorAssembler() \
        .setInputCols(featureColumns) \
        .setOutputCol("features") \
        .setHandleInvalid("skip") #собираем признаки в вектор

    scaler = MinMaxScaler() \
        .setInputCol("features") \
        .setOutputCol("scaledFeatures") #нормализация

    logger.info("Pipeline to data")

    scaled = Pipeline(stages=[
        data_indexer,
        data_encoder,
        assembler,
        scaler,
    ]).fit(data).transform(data) #собрали все стадии в один pipeline

    logger.info("Training and test samples, 80/20")
    tt = scaled.randomSplit([0.8, 0.2]) #разделили датасет на обучающую и тестовуб выборки
    training = tt[0]
    test = tt[1]

    logger.info("LogisticRegression, RegParam=0.2, ElasticNetParam=0.8")
    lr = LogisticRegression()\
        .setMaxIter(1)\
        .setRegParam(0.2)\
        .setElasticNetParam(0.8)\
        .setFamily("binomial")\
        .setFeaturesCol("scaledFeatures")\
        .setLabelCol("tx_fraud") #логистическая регрессия, создали трансформер, параметры заданы на угад

    logger.info("LogisticRegression, fit")
    model = lr.fit(training) #трансофрмер вызвали на обучающу выборку
    logger.info("LogisticRegression, summary")
    model_summary = model.summary #созранили параметры
    logger.info("LogisticRegression, prediction")
    predicted_model = model.transform(test) #предсказали на тестовой выборке
    logger.info("LogisticRegression, metricsList")
    metricsList = udfModelEvaluator(predicted_model, "tx_fraud") #собрали метрики на предсказанном ДФ

    #подбор гиперпараметров trainValidationSplit, стадии одинаковые, как в модели выше
    logger.info("LogisticRegression, ParamGridBuilder")
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(lr.fitIntercept, [False, True]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    #TrainValidationSplit
    logger.info("LogisticRegression, ParamGridBuilder, trainValidationSplit")
    trainValidationSplit = TrainValidationSplit() \
        .setEstimator(lr) \
        .setEvaluator(evaluator) \
        .setEstimatorParamMaps(paramGrid) \
        .setCollectSubModels(True) \
        .setParallelism(2)
    logger.info("TV_LogisticRegression, fit")
    tv_model = trainValidationSplit.fit(training)
    logger.info("TV_LogisticRegression, summary")
    tv_model_summary = tv_model.bestModel.summary
    logger.info("TV_LogisticRegression, prediction")
    predicted_tv_model = tv_model.transform(test)
    logger.info("TV_LogisticRegression, metricsList")
    tv_metricsList = udfModelEvaluator(predicted_tv_model, "tx_fraud")
    logger.info("TV_LogisticRegression, param_map")
    param_map = tv_model.bestModel.extractParamMap()

    #метрики для логистической регрессии
    df_metrics = predicted_model.select('tx_fraud', 'prediction')
    scores = pd.DataFrame(data={"F1": 0.0, "P": 0.0, "R": 0.0, "A": 0.0}, index=range(bootstrap_iterations))

    for i in range(bootstrap_iterations):
        sample = df_metrics.sample(True, 0.1)
        metricsList_sample = udfModelEvaluator(sample, "tx_frad")
        scores.loc[i, "F1"] = metricsList_sample['F1']
        scores.loc[i, "P"] = metricsList_sample['Precision']
        scores.loc[i, "R"] = metricsList_sample['Recall']
        scores.loc[i, "A"] = metricsList_sample['Accuracy']

    #метрики для trainValidationSplit
    df_metrics_tv = predicted_tv_model.select('tx_fraud', 'prediction')
    scores_tv = pd.DataFrame(data={"F1": 0.0, "P": 0.0, "R": 0.0, "A": 0.0}, index=range(bootstrap_iterations))

    for i in range(bootstrap_iterations):
        sample_tv = df_metrics_tv.sample(True, 0.1)
        metricsList_sample_tv = udfModelEvaluator(sample_tv, "tx_fraud")
        scores_tv.loc[i, "F1"] = metricsList_sample_tv['F1']
        scores_tv.loc[i, "P"] = metricsList_sample_tv['Precision']
        scores_tv.loc[i, "R"] = metricsList_sample_tv['Recall']
        scores_tv.loc[i, "A"] = metricsList_sample_tv['Accuracy']

    #F1-score 3-sigma rule
    metrics_all = [scores, scores_tv]
    names = ['LR', 'LR_TV']
    for model in range(len(metrics_all)):
        f1_mean = metrics_all[model]["F1"].mean()
        f1_std = metrics_all[model]["F1"].std()

        f1_low = f1_mean - 3 * f1_std
        f1_upp = f1_mean + 3 * f1_std

        ax = sns.histplot(x=metrics_all[model]["F1"])

        x = np.linspace(f1_low, f1_upp, 100)
        y = norm.pdf(x, loc=f1_mean, scale=f1_std)
        ax.plot(x, y, color="blue")
        ax.axvline(f1_mean, color="red", linestyle='dashed')
        ax.axvline(f1_mean - f1_std, color="green", linestyle='dashed')
        ax.axvline(f1_mean + f1_std, color="green", linestyle='dashed');
        ax.set(title=f'3-sigma rule {names[model]}')

        plt.savefig(f"F1_{names[model]}.png")
        plt.close()

    #метрики для трех моделей на одном графике
    columns_to_plot = ['F1', 'P', 'R', 'A']
    fig, axes = plt.subplots(len(columns_to_plot), 1, figsize=(8, 5 * len(columns_to_plot)))

    for i, column in enumerate(columns_to_plot):
        sns.histplot(data=scores, x=column, ax=axes[i], color='blue', label='scores', alpha=0.3)
        sns.histplot(data=scores_tv, x=column, ax=axes[i], color='green', label='scores', alpha=0.3)

        axes[i].set_xlabel(column)
        axes[i].set_ylabel('Count')
        axes[i].set_title(f'{column}')

        axes[i].legend()

    plt.tight_layout()
    plt.savefig("metr_all_models.png")
    plt.close()

    #t-тест
    alpha = 0.01

    for name in ['F1']:
        # p-value for F1 score
        pvalue = ttest_ind(scores[name], scores_tv[name]).pvalue

        if pvalue < alpha:
            H0 = "Отклонить нулевую гипотезу"
        else:
            H0 = "Принять нулевую гипотезу"

        f1_h0 = f"p-value ({name}): {H0}"

    for model in range(len(metrics_all)):
        mean = metrics_all[model]['F1'].mean()
        std_error = metrics_all[model]['F1'].std() / np.sqrt(len(metrics_all[model]['F1']))
        confidence_level = 0.95

        confidence_interval = stats.t.interval(confidence_level, len(scores_tv) - 1, mean, std_error)
        confidence_interval_str = f'Confidence interval for F1 (LR_TV): {confidence_interval[0]:.4f} <- {scores_tv["F1"].mean():.4f} -> {confidence_interval[1]:.4f}'

    with open('text.txt', 'w') as file:
        for key, value in metricsList.items():
            file.write("LR_model_{0:<16}: {1}\n".format(key, value))

        file.write("#############################################\n")
        for key, value in metricsList_tv.items():
            file.write("LR_TV_model_{0:<16}: {1}\n".format(key, value))

        file.write(
            f"#############################################\np-value for F1 score\n{f1_h0}\n{confidence_interval_str}\n")
        file.write("#############################################\n")
        for i in param_map:
            file.write(f"{i.name:<16}\t{param_map[i]}\n")

    file.close()


    #сохранение pipeline для регистрации модели в Mlflow
    bestML = tv_model.bestModel
    pipeline = Pipeline().setStages([data_indexer, data_encoder, assembler, scaler, selector, bestML])
    [trainingData, testData] = data.randomSplit([0.7, 0.3], seed=42)
    pipelineModel_TV = pipeline.fit(trainingData)
    pipelineModel.write().overwrite().save("pipelineModel_TV") #сохраняем модель


if __name__ == "__main__":
    main()








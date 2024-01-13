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


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))


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

    file_dir = "hdfs://rc1a-dataproc-m-dt27mwn7ayzjtxu7.mdb.yandexcloud.net:8020/data_fraud/2022-06-07.txt"
    file_name = file_dir.split("/")[len(file_dir.split("/")) - 1]

    logger.info(f"Read data {file_name}...")
    rdd = spark.sparkContext.textFile("{0}".format(file_dir))
    header = rdd.take(1)[0]
    df = rdd.filter(lambda r: r != header) \
        .map(lambda r: r.split(',')) \
        .toDF(re.sub(r'\s+', '', header).split('|'))

    df = df.withColumnRenamed('#tranaction_id', 'tranaction_id')

    len_df = df.count()

    logger.info(f"Starting validation {file_name}")
    df_validation = (
        df  
        .filter(col("terminal_id").between(1, 999))
        .filter(col("tx_amount") > 0)
        .filter(col("customer_id") > 0)
        .distinct()
    )

    logger.info(f"Starting remobe null from {file_name}")
    df_validation = df_validation.select([to_null(c).alias(c) for c in df_validation.columns]).na.drop()
    len_val_df = df_validation.count()

    df_info = {}
    df_info.update({"count_del_rows": len_df - len_val_df})

    logger.info(f"Changing column types {file_name}")
    df_validation = df_validation.withColumn('tx_amount', df_validation['tx_amount'].cast(FloatType()))
    df_validation = df_validation.withColumn('terminal_id', df_validation['terminal_id'].cast(IntegerType()))
    df_validation = df_validation.withColumn('tx_fraud', df_validation['tx_fraud'].cast(IntegerType()))
    df_validation = df_validation.withColumn("tx_datetime", f.to_timestamp("tx_datetime", 'yyyy-MM-dd HH:mm:ss'))
    df_validation = df_validation.withColumn('hour_tx_datetime', f.hour(df_validation.tx_datetime))
    df_validation = df_validation.withColumn("tx_datetime", f.from_unixtime(f.unix_timestamp(df_validation.tx_datetime),
                                                                            'yyyy-MM-dd HH:mm:ss'))
    logger.info("Generating a pecent fraud on terminal")

    logger.info("df_cnt_transaction_on_terminal")
    df_cnt_transaction_on_terminal = (
        df_validation
        .select("terminal_id")
        .groupBy("terminal_id")
        .count()
        .withColumnRenamed("count", "cnt_transactions_on_terminal_id")
    )

    logger.info("df_cnt_fraud_transaction_on_terminal")
    df_cnt_fraud_transaction_on_terminal = (
        df_validation
        .filter("tx_fraud = 1")
        .select("terminal_id")
        .groupBy("terminal_id")
        .count()
        .withColumnRenamed("count", "cnt_fraud_transactions_on_terminal_id")
    )

    logger.info("df_fraud_transaction")
    df_fraud_transaction = df_cnt_transaction_on_terminal. \
        join(df_cnt_fraud_transaction_on_terminal, "terminal_id", "left")

    logger.info("column percent_fraud_on_terminal")

    df_fraud_transaction = df_fraud_transaction.withColumn("percent_fraud_on_terminal", f.round(col('cnt_fraud_transactions_on_terminal_id') / col('cnt_transactions_on_terminal_id') * 100, 2))
    df_fraud_transaction = df_fraud_transaction.withColumn('percent_fraud_on_terminal', df_fraud_transaction['percent_fraud_on_terminal'].cast(FloatType()))

    logger.info("column percent_fraud_on_terminal_udf")
    def udf_percent(perc):
        if perc is None:
            return "Unknown"
        elif (perc < 10):
            return "Under 10"
        elif (perc >= 10 and perc <= 25):
            return "Between 10 and 25"
        elif (perc > 25 and perc <= 50):
            return "Between 25,.. and 50"
        elif (perc > 50 and perc <= 75):
            return "Between 50,.. and 50"
        elif (perc > 75):
            return "Over 75"
        else:
            return "NA"

    percent_udf = udf(udf_percent)
    df_fraud_transaction = df_fraud_transaction.withColumn("percent_fraud_on_terminal_udf", percent_udf("percent_fraud_on_terminal"))

    logger.info("Join df_fraud_transaction to df_validation")
    df_validation = df_validation.join(df_fraud_transaction, "terminal_id", "left")

    logger.info(f"Save local to parquet...")
    df_validation.write.mode("overwrite").parquet("hdfs://rc1a-dataproc-m-dt27mwn7ayzjtxu7.mdb.yandexcloud.net:8020/data_fraud/data_{0}.parquet".format(file_name[:-4]))

    spark.stop()
    
    print(df_info)
    print("Done")
if __name__ == "__main__":
    main()
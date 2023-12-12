import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import findspark
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as F
import re
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, asc, desc
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, isnan, when, trim


def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))


def main():
    spark = SparkSession \
        .builder \
        .appName("mlops") \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores", "2") \
        .config("spark.executor.cores", "5") \
        .config("spark.executor.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()

    file_dir = "hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net:8020/data_fraud/2019-09-21.txt"
    file_name = file_dir.split("/")[len(file_dir.split("/")) - 1]

    rdd = spark.sparkContext.textFile("{0}".format(file_dir))
    header = rdd.take(1)[0]
    df = rdd.filter(lambda r: r != header) \
        .map(lambda r: r.split(',')) \
        .toDF(re.sub(r'\s+', '', header).split('|'))

    df = df.withColumnRenamed('#tranaction_id', 'tranaction_id')

    df = df.withColumn('tx_amount', df['tx_amount'].cast(FloatType()))

    len_df = df.count()

    df_validation = (
        df  
        .filter(col("terminal_id").between(1, 999))
        .filter(col("tx_amount") > 0)
        .filter(col("customer_id") > 0)
        .distinct()
    )

    df_validation = df_validation.select([to_null(c).alias(c) for c in df.columns]).na.drop()
    len_val_df = df_validation.count()

    df_info = {}
    df_info.update({"count_del_rows": len_df - len_val_df})

    df_validation.write.parquet(
        "hdfs://rc1a-dataproc-m-68kr0givukqc5yec.mdb.yandexcloud.net:8020/data_fraud/df_validation_{0}.parquet".format(
            file_name[:-4]))

    spark.stop()

    print(df_info)

if __name__ == "__main__":
    main()

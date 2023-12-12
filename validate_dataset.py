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
import numpy as np
import pandas as pd
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, isnan, when, trim




def write_info(info, name_file):
    with open(r"{0}".format(name_file), "w") as file:
        for k, v in info.items():
            file.write('{0} {1} \n'.format(k, v))
            
            
def to_null(c):
    return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))


def main():
    spark = (
        SparkSession
            .builder
            .appName("mlop-validation-dataset")
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
    )
    file_dir = "/user/root/datasets/fraudset/2021-03-14.txt" #задал явно, потому нужно будет через *.txt?
    file_name = file_dir.split("/")[len(file_dir.split("/")) - 1]

    rdd = spark.sparkContext.textFile("{0}".format(file_dir))
    header = rdd.take(1)[0]
    df = rdd.filter(lambda r: r != header) \
            .map(lambda r: r.split(',')) \
            .toDF(re.sub(r'\s+', '', header).split('|'))

    df = df.withColumnRenamed('#tranaction_id', 'tranaction_id')

    df = df.withColumn('tx_amount', df['tx_amount'].cast(FloatType()))
    
    
    len_df = df.count() #количество всех строк
    len_fraud_df = df.filter("tx_fraud = 1").count()
    len_without_fraud = len_df - len_fraud_df
    len_distinct_rows = df.distinct().count()
    
    df_info = {'count_all_rows: ': len_df,
               'count_duplicates_rows: ': len_df - len_distinct_rows,
               'count_fraud_rows: ': len_fraud_df,
               'count_without_fraud_rows: ': len_without_fraud}
                
    string_columns = ['tx_datetime', 'customer_id', 'terminal_id']
    numeric_columns = ['tx_amount']
        
    for index, column in enumerate(df.columns):
        if column in string_columns:
            missing_count = df.filter(col(column).eqNullSafe(None) | col(column).isNull() | (col(column) == 0)).count()
            df_info.update({"missings_{0}".format(column):missing_count})
        if column in numeric_columns:
            missing_count = df.filter((col(column) == 0)).count()
            df_info.update({"missings_{0}".format(column):missing_count})
            
    negative_values = df.filter('tx_amount < 0').count()
    df_info.update({"negative_values_tx_amount":negative_values})
    
    
    df_validation = (
        df
            .filter(col("terminal_id").between('1', '999'))
            .filter(col("tx_amount") > 0)
            .filter(col("customer_id") > 0)
            .distinct()
    )
    
    df_validation = df_validation.select([to_null(c).alias(c) for c in df.columns]).na.drop()
    len_val_df = df_validation.count()
    
    df_info.update({"count_del_rows":len_df - len_val_df})
    
    write_info(df_info, "{0}_info_test.txt".format(file_name))
    
    df_validation.write.parquet("/user/root/datasets/fraudset_parq/df_validation_{0}.parquet".format(file_name[:-4]))
    
    
    spark.stop()

if __name__ == "__main__":
        main()

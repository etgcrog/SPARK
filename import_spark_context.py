from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('spark://172.20.0.3:8080')\
                            .appName('meu-spark')\
                            .getOrCreate()
print(spark)
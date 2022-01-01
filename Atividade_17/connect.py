from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from time import time

if __name__=="__main__":
    conf = pyspark.SparkConf().setAppName('app_do_eduardo').setMaster("spark://35.235.241.161:7077")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # dados = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load("202109_Remuneracao.csv")
    print("\n"*10)
    input()
    
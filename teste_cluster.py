from pyspark.ml.clustering import PowerIterationClustering
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

df = spark.createDataFrame([
    (0, 1, 1.0),
    (0, 2, 1.0),
    (1, 2, 1.0),
    (3, 4, 1.0),
    (4, 0, 0.1)
], ["src", "dst", "weight"])

pic = PowerIterationClustering(k=2, maxIter=20, initMode="degree", weightCol="weight")

pic.assignClusters(df).show()

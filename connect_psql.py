
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/home/edudev/Documents/SoulCode/Spark/postgresql-42.2.6.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.20.0.3:5432/teste_spark") \
    .option("dbtable", "t1") \
    .option("user", "postgres") \
    .option("password", "casa1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()

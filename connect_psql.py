from pyspark.sql import SparkSession

postgres = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/home/edudev/Documents/SoulCode/Spark/postgresql-42.2.6.jar") \
    .getOrCreate()

qry = """create table t2 (name varchar(50), age int)"""
    
result = postgres.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.20.0.3:5432/teste_spark") \
    .option("dbtable", 't1') \
    .option("user", "postgres") \
    .option("password", "casa1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()
    
result.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.20.0.3:5432/teste_spark") \
    .option("dbtable", "create table t2 (id int, name char(50))") \
    .option("user", "postgres") \
    .option("password", "casa1234") \
    .save()

# spark.write.option("createTableColumnTypes", "grau CHAR(64), comments VARCHAR(1024)") \
#     .jdbc("jdbc:postgresql://172.20.0.3:5432/teste_spark", "t1",
#           properties={"user": "postgres", "password": "casa1234"})




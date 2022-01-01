from pyspark.sql import SparkSession
import findspark
#NAO ESTOU USANDO MYSQL CONNECTOR !!!

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

mysql_url="jdbc:mysql://35.199.101.197:3306/desafio"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL With Mysql") \
    .config("spark.jars", "/home/edudev/Documents/SoulCode/Spark/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()
    

df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "orcamentos_despesa") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .load()

df_pandas = df.toPandas()

df_com_uuid = df_pandas.assign(id='uuid()')
coluna = df_com_uuid.columns.tolist()
coluna = coluna[-1:] + coluna[:-1]
df_com_uuid = df_com_uuid[coluna]
print(df_com_uuid.head())

df = spark.createDataFrame(df_com_uuid)

df.write.parquet("/home/edudev/Documents/SoulCode/Spark/parquet_mysql/mysql.parquet")





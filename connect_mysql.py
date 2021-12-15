from pyspark.sql import SparkSession

mysql_url="jdbc:mysql://172.17.0.3:3306/oldtech"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL With Mysql") \
    .config("spark.jars", "/home/edudev/Documents/SoulCode/Spark/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()
    
df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "oldtech") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .load()

df.write()



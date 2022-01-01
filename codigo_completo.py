from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import datetime
import findspark

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

mysql_url="jdbc:mysql://35.199.101.197:3306/desafio"

spark1 = SparkSession \
    .builder \
    .appName("Python Spark SQL With Mysql") \
    .config("spark.jars", "/home/edudev/Documents/SoulCode/Spark/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()

df = spark1.read \
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

df = spark1.createDataFrame(df_com_uuid)

df.write.parquet("/home/edudev/Documents/SoulCode/Spark/parquet_mysql/mysql.parquet")

spark2 = SparkSession.builder.master('local').appName('spark-cassandra').getOrCreate()

start = datetime.datetime.now()

df = spark2.read.parquet('/home/edudev/Documents/SoulCode/Spark/parquet_mysql/mysql.parquet')

cluster = Cluster(['34.151.227.5'])
session = cluster.connect('desafio')
for i in df.collect():
    session.execute(f"""insert into orcamentos_despesa (id, ano, cod_orgao, nome_orgao, cod_sub, nome_sub, cod_unidade, nome_unidade, cod_funcao, nome_funcao, cod_subfuncao, nome_subfuncao, cod_programa, nome_programa, cod_acao, nome_acao, cod_grp_despesa, nome_grt_despesa, cod_elemento, nome_despesa, orcamento_inicial, orcamento_atualizado, orcamento_empenhado, orcamento_realizado) values ({i[0].replace("'", "")}, '{i[1]}', '{i[2]}','{i[3]}','{i[4]}','{i[5]}','{i[6]}','{i[7]}','{i[8]}','{i[9]}','{i[10]}','{i[11]}','{i[12]}','{i[13]}','{i[14]}','{i[15]}','{i[16]}','{i[17]}','{i[18]}','{i[19]}', {i[20]},{i[21]},{i[22]},{i[23]})""")
    
end = datetime.datetime.now()
print(end - start)





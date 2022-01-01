from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

start = time.time()

df = spark.read.parquet('/home/edudev/Documents/SoulCode/Spark/parquet_mysql/mysql.parquet')

# df_pandas = df.toPandas()
# print(df_pandas.iloc[407])
# print(df_pandas.shape)

cluster = Cluster(['34.151.227.5'])
session = cluster.connect('desafio')
for i in df.collect():
    session.execute(f"""insert into orcamentos_despesa (id, ano, cod_orgao, nome_orgao, cod_sub, nome_sub, cod_unidade, nome_unidade, cod_funcao, nome_funcao, cod_subfuncao, nome_subfuncao, cod_programa, nome_programa, cod_acao, nome_acao, cod_grp_despesa, nome_grt_despesa, cod_elemento, nome_despesa, orcamento_inicial, orcamento_atualizado, orcamento_empenhado, orcamento_realizado) values ({i[0].replace("'", "")}, '{i[1]}', '{i[2]}','{i[3]}','{i[4]}','{i[5]}','{i[6]}','{i[7]}','{i[8]}','{i[9]}','{i[10]}','{i[11]}','{i[12]}','{i[13]}','{i[14]}','{i[15]}','{i[16]}','{i[17]}','{i[18]}','{i[19]}', {i[20]},{i[21]},{i[22]},{i[23]})""")
end = time.time()
print(end - start)
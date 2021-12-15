from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc, concat, lit, regexp_replace
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

#carregando 
df_junho = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/junho.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
df_agosto = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/agosto.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
df_setembro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/setembro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
df_outubro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/outubro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')

#selecionando so as colunas de interesse
df_tratado_junho = df_junho.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
df_tratado_agosto = df_agosto.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
df_tratado_setembro = df_setembro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
df_tratado_outubro = df_outubro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')

#dropando os valores nulos
# ---------- df_tratado.filter(df_tratado['UF'].isNull()).count() ----- quantidade de nulos
df_tratado_junho = df_tratado_junho.na.drop()
df_tratado_agosto = df_tratado_agosto.na.drop()
df_tratado_setembro = df_tratado_setembro.na.drop()
df_tratado_outubro = df_tratado_outubro.na.drop()
# concatenando os dataframes
# QUANTIDADE DE REGISTROS = 208.527.091  208 MILHOES
#df = df_tratado_junho.select(concat(col(df_tratado_agosto), col(df_tratado_setembro), col(df_tratado_outubro)))
temp1 = df_tratado_junho.union(df_tratado_agosto)
temp2 = temp1.union(df_tratado_setembro)
df = temp2.union(df_tratado_outubro)
#renomeando as colunas
df = df.select(col("UF").alias("uf") \
,col("MÊS DISPONIBILIZAÇÃO").alias("mes") \
,col("NOME BENEFICIÁRIO").alias("nome") \
,col("NIS RESPONSÁVEL").alias("nis")\
,col("PARCELA").alias("numero_parcela")\
,col("VALOR BENEFÍCIO").alias('valor'))
#trocando as virgulas por ponto
new_df = df.withColumn("valor",  regexp_replace("valor",  ","  ,"."))
new_df2 = new_df.withColumn("valor", col("valor").cast(FloatType()))
#agrupando por nome
estatisticas = df.groupBy('nome')\
.agg(max("valor").alias('maximo')
, min("valor").alias('minimo')\
, avg("valor").alias('media')\
,count("valor").alias('quantidade'))
estatisticas.show(10, truncate = False)
#filtrando por quantidade de auxilios recebidos
stats_2 = estatisticas.filter(estatisticas.quantidade >= 2)
stats_2.orderBy(col('quantidade').desc()).show(truncate=False)








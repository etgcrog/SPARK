from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc, concat, lit, regexp_replace, sum
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

df = spark.read.csv("/media/edudev/_home/2020/abril.csv",\
header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')

df = df.select(col("UF").alias("uf") \
,col("MÊS DISPONIBILIZAÇÃO").alias("mes") \
,col("NOME BENEFICIÁRIO").alias("nome") \
,col("CPF RESPONSÁVEL").alias("cpf") \
,col("NIS RESPONSÁVEL").alias("nis")\
,col("PARCELA").alias("numero_parcela")\
,col("VALOR BENEFÍCIO").alias('valor'))

# df.write.parquet("/home/edudev/Documents/SparkParquets")
df = spark.read.parquet('/home/edudev/Documents/SparkParquets')

df = df.withColumn("valor",  regexp_replace("valor",  ","  ,"."))
df = df.withColumn("valor", col("valor").cast(FloatType()))

estatisticas = df.groupBy('nome', 'cpf')\
.agg(max("valor").alias('maximo')
, min("valor").alias('minimo')\
, avg("valor").alias('media')\
, sum("valor").alias('total_recebido')\
,count("valor").alias('quantidade'))

df.createOrReplaceTempView('view_df')
spark.sql('select nome, count(*), sum(valor) as quantidade from view_df group by nome, cpf order by quantidade desc').show()
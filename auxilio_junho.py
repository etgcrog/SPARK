from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc, concat, lit, regexp_replace, sum
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

df_agosto = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/agosto.csv",\
header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')

df_tratado_agosto = df_agosto.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')

df = df_tratado_agosto.na.drop()

df = df.select(col("UF").alias("uf") \
,col("MÊS DISPONIBILIZAÇÃO").alias("mes") \
,col("NOME BENEFICIÁRIO").alias("nome") \
,col("CPF RESPONSÁVEL").alias("cpf") \
,col("NIS RESPONSÁVEL").alias("nis")\
,col("PARCELA").alias("numero_parcela")\
,col("VALOR BENEFÍCIO").alias('valor'))

df = df.withColumn("valor",  regexp_replace("valor",  ","  ,"."))
df = df.withColumn("valor", col("valor").cast(FloatType()))

estatisticas = df.groupBy('nome', 'cpf')\
.agg(max("valor").alias('maximo')
, min("valor").alias('minimo')\
, avg("valor").alias('media')\
, sum("valor").alias('total_recebido')\
,count("valor").alias('quantidade'))

estatisticas.show(100, truncate = False)

estatisticas.filter(estatisticas['nome'] == "EDUARDO TELES GUIMARAES").show(truncate=False)


print(estatisticas.count())

estatisticas.orderBy(col('quantidade').desc()).show()
qnt_maior_3 = estatisticas.filter(estatisticas['quantidade'] > 3).count()
qnt_maior_3 = estatisticas.filter(estatisticas['quantidade'] > 3).count()
print(f"Quantidade de pessoas com mais de 3 parcelas = {qnt_maior_3}")



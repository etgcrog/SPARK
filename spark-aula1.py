from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

df1 = spark.read.csv("/home/edudev/Documents/SoulCode/Spark/lendo_csv/Sistema_A_SQL.csv", header=True, inferSchema=True)
df1 = df1.na.drop()

estatisticas = df1.groupBy('vendedor')\
    .agg(max("total").alias('maximo')
    , min("total").alias('minimo')\
    , avg("total").alias('media')\
    ,count("total").alias('quantidade'))
estatisticas.show()

stats_2 = estatisticas.filter(estatisticas.quantidade >= 2)
stats_2.orderBy(col('quantidade').desc()).show(truncate=False)

  
# estatisticas.filter(estatisticas['quantidade'] > 2).show()
# desvio = df1.agg(stddev('total').alias("Desvio")).collect()
# print(desvio)

# media = df1.agg(avg('total').alias('Media de Vendas'))
# media.show()

# max_total = df1.agg(max('total').alias('Max de Vendas'))
# max_total.show()

# min_total = df1.agg(min('total').alias('Min de Vendas'))
# min_total.show()

# dado = [(df1.approxQuantile("total", [0.5], 0.25))]

# mediana = spark.createDataFrame(dado, ['Mediana'])
# mediana.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dados = [("teste", 123, ''), ('teste2', 456, '')]
df = spark.createDataFrame(dados, ['column', 'valores', 'extra'])
df.show()

#acessando o valor da coluna 0
for i in df.collect():
    print(i[0])
    
print(df.count())
print(df.take(1))

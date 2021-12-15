from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, asc, desc, count

spark = SparkSession.builder.master('local').appName('meu-spark').getOrCreate()

df = spark.read.csv("/home/edudev/Documents/SoulCode/Spark/IMDb movies.csv", header=True, inferSchema=True)

df_group = df.groupBy('country').count()
# df_group.show()

#mudando a coluna vote para inteiro
df.printSchema()
df_vote = df.select('title', 'country', 'year', 'votes')
df_vote_int = df_vote.withColumn('votosINT', df_vote.votes.cast('int')).drop('votes')
df_vote_int.show()
#criando uma view e ordenando por pais e ordenando com view
df_vote_int.createOrReplaceTempView('movies')
spark.sql('select country, count(*) as qtd from movies GROUP BY country ORDER BY qtd desc').show()
#mesmo resultado em pyspark
df_qnt = df_vote_int.groupBy('country').count()
df_qnt.orderBy(col('count').desc()).show()

##################### MESMO RESULTADO ##################################
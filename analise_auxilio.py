from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc

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
df_tratado_junho = df_tratado_junho.na.drop()
df_tratado_agosto = df_tratado_agosto.na.drop()
df_tratado_setembro = df_tratado_setembro.na.drop()
df_tratado_outubro = df_tratado_outubro.na.drop()


# df_tratado.show(truncate=False)
# #Identicar quantidade de nulos - NAO VAMOS DROPAR
# df_tratado.filter(df_tratado['UF'].isNull()).count()




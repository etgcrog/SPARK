from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc, concat, lit, regexp_replace
from pyspark.sql.types import FloatType
from datetime import datetime

"""
[Dicionário de Dados - Auxílio Emergencial]

MÊS DISPONIBILIZAÇÃO -Ano/Mês a que se refere a parcela, no formato AAAAMM.

UF -Sigla da Unidade Federativa do beneficiário do Auxílio Emergencial.
CÓDIGO MUNICÍPIO IBGE - Código, no IBGE (Instituto Brasileiro de Geografia e Estatistica), do município do beneficiário do Auxílio Emergencial.
NOME MUNICÍPIO	Nome do município do beneficiário do Auxílio Emergencial.
NIS BENEFICIÁRIO - Número de Identificação Social (NIS) do beneficiário do Auxílio Emergencial, caso possua.
CPF BENEFICIÁRIO - Número no Cadastro de Pessoas Físicas (CPF) do beneficiário do Auxílio Emergencial, caso possua.
NOME BENEFICIÁRIO - Nome do beneficiário do Auxílio Emergencial.
NIS RESPONSÁVEL - Número de Identificação Social (NIS) do responsável pelo beneficiário do Auxílio Emergencial, caso possua.
CPF RESPONSÁVEL - Número no Cadastro de Pessoas Físicas (CPF) do responsável pelo beneficiário do Auxílio Emergencial, caso possua.
NOME RESPONSÁVEL - Nome do responsável pelo beneficiário do Auxílio Emergencial, caso possua.
ENQUADRAMENTO - Identifica se o beneficiário é do grupo Bolsa Família, Inscrito no Cadastro Único (CadÚnico) ou Não Inscrito no Cadastro Único (ExtraCad).
PARCELA - Número sequencial da parcela disponibilizada.
OBSERVAÇÃO - Indica alterações na parcela disponibilizada como, por exemplo, se foi devolvida ou está retida.
VALOR BENEFÍCIO - Valor disponibilizado na parcela.
"""
i = datetime.now()

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

# concatenando os dataframes
temp1 = df_tratado_junho.union(df_tratado_agosto)
temp2 = temp1.union(df_tratado_setembro)
df = temp2.union(df_tratado_outubro)

# QUANTIDADE DE REGISTROS = 208.527.091  208 MILHOES

#dropando os valores nulos
df = df.na.drop()
# ---------- df_tratado.filter(df_tratado['UF'].isNull()).count() ----- quantidade de nulos
# df_tratado_junho = df_tratado_junho.na.drop()
# df_tratado_agosto = df_tratado_agosto.na.drop()
# df_tratado_setembro = df_tratado_setembro.na.drop()
# df_tratado_outubro = df_tratado_outubro.na.drop()

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
estatisticas = new_df2.groupBy('nome')\
.agg(max("valor").alias('maximo')
, min("valor").alias('minimo')\
, avg("valor").alias('media')\
,count("valor").alias('quantidade'))
estatisticas.show(10, truncate = False)

#filtrando por quantidade de auxilios recebidos
estatisticas.filter(estatisticas['nome'] == "EDUARDO TELES GUIMARAES").show(truncate=False)

f = datetime.now()
print(f-i)







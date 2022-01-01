from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import stddev, avg, max, min, col, expr, count, desc, concat, lit, regexp_replace, sum
from pyspark.sql.types import FloatType
from datetime import datetime

if __name__=="__main__":
    conf = pyspark.SparkConf().setAppName('app_do_eduardo').setMaster("spark://34.151.254.68:7077")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # dados = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load("202109_Remuneracao.csv")
    print("\n"*10)
    input()

    """
    [Dicionário de Dados - Auxílio Emergencial - Abril - Outubro de 2020]

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
    df_abril = spark.read.csv("/media/edudev/_home/2020/abril.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_maio = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/maio.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_junho = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/junho.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_julho = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/julho.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_agosto = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/agosto.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_setembro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/setembro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_outubro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/outubro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_novembro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/novembro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')
    df_dezembro = spark.read.csv("/media/edudev/_home/auxilio_emegencial_csv/dezembro.csv",\
    header=True, inferSchema=True, sep=';', encoding='ISO-8859-1')


    #selecionando so as colunas de interesse
    df_tratado_abril = df_abril.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_maio = df_maio.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_junho = df_junho.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO','CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_julho = df_julho.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_agosto = df_agosto.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_setembro = df_setembro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_outubro = df_outubro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_novembro = df_novembro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')
    df_tratado_dezembro = df_dezembro.select('UF', 'MÊS DISPONIBILIZAÇÃO', 'NOME BENEFICIÁRIO', 'CPF RESPONSÁVEL', 'NIS RESPONSÁVEL', 'PARCELA', 'VALOR BENEFÍCIO')

    # concatenando os dataframes
    temp1 = df_tratado_abril.union(df_tratado_maio)
    temp2 = temp1.union(df_tratado_junho)
    temp3 = temp2.union(df_tratado_julho)
    temp4 = temp3.union(df_tratado_agosto)
    temp5 = temp4.union(df_tratado_setembro)
    temp6 = temp5.union(df_tratado_outubro)
    df = temp2.union(df_tratado_novembro)

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
    ,col("CPF RESPONSÁVEL").alias("cpf") \
    ,col("NIS RESPONSÁVEL").alias("nis")\
    ,col("PARCELA").alias("numero_parcela")\
    ,col("VALOR BENEFÍCIO").alias('valor'))

    #trocando as virgulas por ponto
    new_df = df.withColumn("valor",  regexp_replace("valor",  ","  ,"."))
    new_df2 = new_df.withColumn("valor", col("valor").cast(FloatType()))

    #agrupando por nome e cpf
    estatisticas = new_df2.groupBy('nome', 'cpf')\
    .agg(max("valor").alias('maximo')
    , min("valor").alias('minimo')\
    , avg("valor").alias('media')\
    , sum("valor").alias('total_recebido')\
    ,count("valor").alias('quantidade'))
    estatisticas.show(10, truncate = False)

    #filtrando por nome especifico

    estatisticas.filter(estatisticas['nome'] == "EDUARDO TELES GUIMARAES").show(truncate=False)
    estatisticas.filter(estatisticas['nome'] == "BEATRIZ BRITO ALVES").show(truncate=False)

    #identificando outliers!!!!!!!!!!!!
    estatisticas.orderBy(col('quantidade').desc()).filter(estatisticas['quantidade'] > 3).show()
    qnt_maior_3 = estatisticas.orderBy(col('quantidade').desc()).filter(estatisticas['quantidade'] > 3).count()
    print(f"A quantidade de pessoas acima de 3 parcelas foi de -> {qnt_maior_3}")
    #criando view
    view_estatisticas = estatisticas.createTempView('view_estatisticas')
    spark.sql('select nome, count(*) as qtd from view_estatisticas GROUP BY nome ORDER BY qtd desc').show()

    #tempo de processos
    f = datetime.now()
    print(f-i)
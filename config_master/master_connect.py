from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# spark-class org.apache.spark.deploy.master.Master
#pegar o endereco "spark://192.168.0.15:7077" e executar no worker/cluster
# Executar esse comando no slave
# start-worker.sh spark://192.168.0.15:7077

# Registering worker 192.168.0.12:42041 with 2 cores, 2.7 GiB RAM

# spark = SparkSession \
# .builder\
# .appName("spark-standalone-cluster")\
# .master("spark://192.168.0.15:7077")\
# .config("spark.driver.memory","4g")\
# .config("spark.driver.cores",'4')\
# .config("spark.executor.memory", '2.7g')\
# .config("spark.executor.cores", '2')\
# .config("spark.cores.max", '4')\
# .getOrCreate()


spark = SparkSession \
.builder\
.appName("spark-standalone-cluster")\
.master("spark://192.168.0.15:7077")\
.config("spark.driver.memory","2.5g")\
.config("spark.driver.cores",'2.5')\
.config("spark.executor.memory", '2g')\
.config("spark.executor.cores", '2.5')\
.config("spark.cores.max", '2.5')\
.getOrCreate()

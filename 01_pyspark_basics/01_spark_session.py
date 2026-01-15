"""
PySpark - Criando SparkSession e configurações básicas
Tópicos: SparkSession, configurações, contexto
"""

from pyspark.sql import SparkSession

# ============================================================================
# CRIANDO SPARK SESSION - Forma básica
# ============================================================================
spark = SparkSession.builder \
    .appName("EstudoSpark") \
    .getOrCreate()

# ============================================================================
# SPARK SESSION COM CONFIGURAÇÕES AVANÇADAS
# ============================================================================
spark_configured = SparkSession.builder \
    .appName("EstudoSparkConfigurado") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# ============================================================================
# SPARK SESSION PARA HIVE
# ============================================================================
spark_hive = SparkSession.builder \
    .appName("SparkComHive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# ============================================================================
# SPARK SESSION PARA AWS/S3
# ============================================================================
spark_aws = SparkSession.builder \
    .appName("SparkAWS") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# ============================================================================
# VERIFICANDO CONFIGURAÇÕES
# ============================================================================
print("Spark Version:", spark.version)
print("App Name:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)
print("Default Parallelism:", spark.sparkContext.defaultParallelism)

# Listar todas as configurações
for conf in spark.sparkContext.getConf().getAll():
    print(f"{conf[0]} = {conf[1]}")

# ============================================================================
# SPARK CONTEXT (baixo nível - RDDs)
# ============================================================================
sc = spark.sparkContext

# Criar RDD simples
rdd = sc.parallelize([1, 2, 3, 4, 5])
print("RDD Count:", rdd.count())
print("RDD Collect:", rdd.collect())

# ============================================================================
# FINALIZANDO
# ============================================================================
spark.stop()

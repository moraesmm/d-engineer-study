"""
PySpark - DataFrames e Transformações
Tópicos: criar DF, transformações, actions, schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce,
    upper, lower, trim, concat, concat_ws, substring,
    year, month, dayofmonth, date_format, current_date, datediff,
    sum, avg, count, min, max, countDistinct,
    explode, split, array, struct,
    row_number, rank, dense_rank, lead, lag,
    broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, ArrayType
)

spark = SparkSession.builder.appName("DataFramesTransformations").getOrCreate()

# ============================================================================
# CRIANDO DATAFRAMES
# ============================================================================

# 1. A partir de lista
data = [
    (1, "João", "Vendas", 5000.0, "2023-01-15"),
    (2, "Maria", "TI", 7500.0, "2022-06-20"),
    (3, "Pedro", "Vendas", 4500.0, "2023-03-10"),
    (4, "Ana", "RH", 6000.0, "2021-11-05"),
    (5, "Carlos", "TI", 8000.0, "2020-08-22"),
]

columns = ["id", "nome", "departamento", "salario", "data_contratacao"]
df = spark.createDataFrame(data, columns)
df.show()

# 2. Com schema explícito
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("nome", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("salario", DoubleType(), True),
    StructField("data_contratacao", StringType(), True),
])

df_schema = spark.createDataFrame(data, schema)
df_schema.printSchema()

# 3. Lendo de arquivos
# df_csv = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
# df_json = spark.read.json("path/to/file.json")
# df_parquet = spark.read.parquet("path/to/file.parquet")

# ============================================================================
# TRANSFORMAÇÕES BÁSICAS
# ============================================================================

# SELECT
df.select("nome", "salario").show()
df.select(col("nome"), col("salario") * 1.1).show()

# FILTER / WHERE
df.filter(col("salario") > 5000).show()
df.where((col("departamento") == "TI") & (col("salario") >= 7000)).show()

# WITHCOLUMN - adicionar/modificar colunas
df_transformed = df \
    .withColumn("salario_anual", col("salario") * 12) \
    .withColumn("bonus", col("salario") * 0.1) \
    .withColumn("nome_upper", upper(col("nome"))) \
    .withColumn("data_atual", current_date())

df_transformed.show()

# WITHCOLUMNRENAMED
df.withColumnRenamed("nome", "funcionario").show()

# DROP
df.drop("data_contratacao").show()

# ============================================================================
# CONDICIONAIS - WHEN/OTHERWISE
# ============================================================================

df_categoria = df.withColumn(
    "categoria_salario",
    when(col("salario") < 5000, "Junior")
    .when(col("salario") < 7000, "Pleno")
    .otherwise("Senior")
)
df_categoria.show()

# COALESCE - primeiro valor não nulo
df.withColumn("dept_default", coalesce(col("departamento"), lit("Sem Dept"))).show()

# ============================================================================
# AGREGAÇÕES
# ============================================================================

# GroupBy simples
df.groupBy("departamento").agg(
    count("*").alias("total_funcionarios"),
    sum("salario").alias("total_salarios"),
    avg("salario").alias("media_salario"),
    min("salario").alias("menor_salario"),
    max("salario").alias("maior_salario")
).show()

# Múltiplas agregações
df.groupBy("departamento").agg(
    countDistinct("nome").alias("funcionarios_unicos"),
    (sum("salario") / count("*")).alias("media_manual")
).show()

# ============================================================================
# WINDOW FUNCTIONS
# ============================================================================

# Definindo window
window_dept = Window.partitionBy("departamento").orderBy(col("salario").desc())

df_window = df.withColumn("rank_salario", rank().over(window_dept)) \
    .withColumn("dense_rank_salario", dense_rank().over(window_dept)) \
    .withColumn("row_num", row_number().over(window_dept))

df_window.show()

# Lead e Lag
window_ordered = Window.orderBy("salario")
df.withColumn("proximo_salario", lead("salario", 1).over(window_ordered)) \
    .withColumn("salario_anterior", lag("salario", 1).over(window_ordered)) \
    .show()

# Running total
window_running = Window.partitionBy("departamento").orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("salario_acumulado", sum("salario").over(window_running)).show()

# ============================================================================
# JOINS
# ============================================================================

departamentos = spark.createDataFrame([
    ("Vendas", "São Paulo"),
    ("TI", "Rio de Janeiro"),
    ("RH", "Belo Horizonte"),
], ["departamento", "cidade"])

# Inner Join
df.join(departamentos, "departamento", "inner").show()

# Left Join
df.join(departamentos, "departamento", "left").show()

# Right Join
df.join(departamentos, "departamento", "right").show()

# Full Outer Join
df.join(departamentos, "departamento", "full").show()

# Broadcast Join (para tabelas pequenas - otimização)
df.join(broadcast(departamentos), "departamento").show()

# Join com condição diferente
df.join(
    departamentos,
    df.departamento == departamentos.departamento,
    "left"
).drop(departamentos.departamento).show()

# ============================================================================
# ORDENAÇÃO E LIMITES
# ============================================================================

df.orderBy("salario").show()
df.orderBy(col("salario").desc()).show()
df.orderBy("departamento", col("salario").desc()).show()

df.limit(3).show()

# ============================================================================
# DISTINCT E DROP DUPLICATES
# ============================================================================

df.select("departamento").distinct().show()
df.dropDuplicates(["departamento"]).show()

# ============================================================================
# UNION
# ============================================================================

novos_funcionarios = spark.createDataFrame([
    (6, "Julia", "TI", 7000.0, "2024-01-10"),
], columns)

df_unido = df.union(novos_funcionarios)
df_unido.show()

# Union por nome de coluna (mais seguro)
df_unido_by_name = df.unionByName(novos_funcionarios)

# ============================================================================
# ACTIONS (executam o DAG)
# ============================================================================

print("Count:", df.count())
print("First:", df.first())
print("Take 3:", df.take(3))
print("Collect:", df.collect())  # Cuidado com datasets grandes!

# Estatísticas
df.describe("salario").show()
df.summary().show()

# ============================================================================
# CACHE E PERSIST
# ============================================================================

from pyspark.storagelevel import StorageLevel

df.cache()  # Equivalente a persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.unpersist()

# ============================================================================
# REPARTITION E COALESCE
# ============================================================================

# Repartition - aumenta ou diminui partições (shuffle completo)
df_repartitioned = df.repartition(4)
df_by_dept = df.repartition("departamento")  # Particiona por coluna

# Coalesce - apenas diminui partições (sem shuffle)
df_coalesced = df.coalesce(1)

print("Partições originais:", df.rdd.getNumPartitions())
print("Após repartition:", df_repartitioned.rdd.getNumPartitions())

# ============================================================================
# EXPLAIN - Ver plano de execução
# ============================================================================

df.filter(col("salario") > 5000).groupBy("departamento").count().explain(True)

spark.stop()

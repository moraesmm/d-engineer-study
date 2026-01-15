"""
Formatos de Arquivo e Compressão no Spark
Tópicos: Parquet, ORC, Avro, Delta, compressão, otimizações
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder \
    .appName("CompressionFormats") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# ============================================================================
# DATASET DE EXEMPLO
# ============================================================================

data = [
    (1, "João", "Vendas", 5000.0, "2023-01-15"),
    (2, "Maria", "TI", 7500.0, "2022-06-20"),
    (3, "Pedro", "Vendas", 4500.0, "2023-03-10"),
    (4, "Ana", "RH", 6000.0, "2021-11-05"),
    (5, "Carlos", "TI", 8000.0, "2020-08-22"),
]

columns = ["id", "nome", "departamento", "salario", "data_contratacao"]
df = spark.createDataFrame(data, columns)

output_base = "c:/Users/morae/dev/data-engineering-study/data/formats"

# ============================================================================
# PARQUET - Formato colunar (RECOMENDADO)
# ============================================================================

"""
PARQUET:
- Formato colunar - excelente para analytics
- Suporta schema evolution
- Predicate pushdown (filtros são aplicados antes de ler todos os dados)
- Compressão por coluna
- Melhor para: leitura de poucas colunas de muitas linhas

Codecs de compressão:
- snappy: rápido, boa compressão (DEFAULT)
- gzip: maior compressão, mais lento
- lz4: muito rápido, compressão moderada
- zstd: bom balanço velocidade/compressão
- none: sem compressão
"""

# Parquet com Snappy (default)
df.write \
    .mode("overwrite") \
    .parquet(f"{output_base}/parquet_snappy")

# Parquet com GZIP (maior compressão)
df.write \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .parquet(f"{output_base}/parquet_gzip")

# Parquet com ZSTD
df.write \
    .mode("overwrite") \
    .option("compression", "zstd") \
    .parquet(f"{output_base}/parquet_zstd")

# Parquet sem compressão
df.write \
    .mode("overwrite") \
    .option("compression", "none") \
    .parquet(f"{output_base}/parquet_none")

# Lendo Parquet
df_parquet = spark.read.parquet(f"{output_base}/parquet_snappy")
df_parquet.show()

# ============================================================================
# ORC - Optimized Row Columnar
# ============================================================================

"""
ORC:
- Também colunar, otimizado para Hive
- Melhor compressão que Parquet em alguns casos
- Suporta ACID transactions (com Hive)
- Codecs: zlib, snappy, lzo, none
"""

df.write \
    .mode("overwrite") \
    .orc(f"{output_base}/orc_default")

df.write \
    .mode("overwrite") \
    .option("compression", "zlib") \
    .orc(f"{output_base}/orc_zlib")

# Lendo ORC
df_orc = spark.read.orc(f"{output_base}/orc_default")
df_orc.show()

# ============================================================================
# JSON
# ============================================================================

"""
JSON:
- Formato texto, legível
- Sem compressão nativa (mas pode usar gzip externo)
- Schema flexível
- Mais lento para processar
- Bom para: integração com APIs, debugging
"""

df.write \
    .mode("overwrite") \
    .json(f"{output_base}/json_default")

# JSON comprimido
df.write \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .json(f"{output_base}/json_gzip")

# Lendo JSON
df_json = spark.read.json(f"{output_base}/json_default")

# Com schema explícito (mais rápido)
schema = StructType([
    StructField("id", IntegerType()),
    StructField("nome", StringType()),
    StructField("departamento", StringType()),
    StructField("salario", DoubleType()),
    StructField("data_contratacao", StringType()),
])
df_json_schema = spark.read.schema(schema).json(f"{output_base}/json_default")

# ============================================================================
# CSV
# ============================================================================

"""
CSV:
- Formato texto, universal
- Sem schema nativo
- Bom para: import/export, compatibilidade
"""

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_base}/csv_default")

# CSV com opções
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("quote", '"') \
    .option("escape", "\\") \
    .option("compression", "gzip") \
    .csv(f"{output_base}/csv_options")

# Lendo CSV
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{output_base}/csv_default")

# ============================================================================
# AVRO - Row-based
# ============================================================================

"""
AVRO:
- Formato baseado em linhas (row-based)
- Schema embutido
- Bom para: streaming, serialização, Kafka
- Requer: spark-avro package

Para usar Avro, adicione a dependência:
--packages org.apache.spark:spark-avro_2.12:3.x.x
"""

# df.write \
#     .mode("overwrite") \
#     .format("avro") \
#     .save(f"{output_base}/avro_default")

# df_avro = spark.read.format("avro").load(f"{output_base}/avro_default")

# ============================================================================
# DELTA LAKE
# ============================================================================

"""
DELTA LAKE:
- Baseado em Parquet + Transaction Log
- ACID transactions
- Schema enforcement e evolution
- Time travel (histórico de versões)
- Requer: delta-spark package

Para usar Delta, adicione a dependência:
--packages io.delta:delta-spark_2.12:3.x.x
"""

# spark_delta = SparkSession.builder \
#     .appName("Delta") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

# df.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .save(f"{output_base}/delta_table")

# Time travel
# spark.read.format("delta").option("versionAsOf", 0).load(path)
# spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)

# ============================================================================
# COMPARAÇÃO DE FORMATOS
# ============================================================================

"""
COMPARAÇÃO DE FORMATOS:

| Formato  | Tipo      | Compressão | Schema    | Use Case                    |
|----------|-----------|------------|-----------|------------------------------|
| Parquet  | Colunar   | Excelente  | Embutido  | Analytics, Data Lake         |
| ORC      | Colunar   | Excelente  | Embutido  | Hive, Analytics              |
| Avro     | Linhas    | Boa        | Embutido  | Streaming, Kafka             |
| JSON     | Texto     | Ruim       | Flexível  | APIs, Debug                  |
| CSV      | Texto     | Ruim       | Nenhum    | Import/Export                |
| Delta    | Colunar   | Excelente  | Versionado| ACID, Time Travel            |

REGRAS DE ESCOLHA:

1. Analytics/Data Lake: Parquet ou Delta
2. Hive/Hadoop tradicional: ORC
3. Streaming/Kafka: Avro
4. Integração externa: JSON ou CSV
5. ACID necessário: Delta Lake
"""

# ============================================================================
# CONFIGURAÇÕES GLOBAIS DE COMPRESSÃO
# ============================================================================

# Configurar compressão padrão para Parquet
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")  # ou gzip, zstd

# Configurar compressão padrão para ORC
spark.conf.set("spark.sql.orc.compression.codec", "snappy")

# Ver configurações atuais
print("Parquet compression:", spark.conf.get("spark.sql.parquet.compression.codec"))

# ============================================================================
# OTIMIZAÇÕES DE LEITURA
# ============================================================================

# Predicate Pushdown - filtros são enviados para o storage
df_filtered = spark.read.parquet(f"{output_base}/parquet_snappy") \
    .filter(col("salario") > 5000)
df_filtered.explain(True)  # Veja "PushedFilters"

# Column Pruning - só lê colunas necessárias
df_columns = spark.read.parquet(f"{output_base}/parquet_snappy") \
    .select("nome", "salario")
df_columns.explain(True)

# Merge Schema (para evolução de schema)
df_merged = spark.read \
    .option("mergeSchema", "true") \
    .parquet(f"{output_base}/parquet_*")

# ============================================================================
# SAVEMODE OPTIONS
# ============================================================================

"""
Modos de escrita:
- overwrite: substitui se existir
- append: adiciona aos dados existentes
- ignore: não faz nada se existir
- error/errorifexists: erro se existir (default)
"""

df.write.mode("overwrite").parquet(f"{output_base}/test_mode")
df.write.mode("append").parquet(f"{output_base}/test_mode")
df.write.mode("ignore").parquet(f"{output_base}/test_mode")
# df.write.mode("error").parquet(f"{output_base}/test_mode")  # Erro!

spark.stop()

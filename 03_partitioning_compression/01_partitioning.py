"""
Particionamento e Bucketing no Spark
Tópicos: partitionBy, bucketBy, repartition, coalesce, estratégias
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, date_format, rand
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("Partitioning") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ============================================================================
# CRIANDO DATASET DE EXEMPLO MAIOR
# ============================================================================

def generate_sample_data(n_records=10000):
    departamentos = ["Vendas", "TI", "RH", "Financeiro", "Marketing"]
    cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre", "Curitiba"]

    data = []
    base_date = datetime(2022, 1, 1)

    for i in range(n_records):
        data.append((
            i,
            f"Funcionario_{i}",
            random.choice(departamentos),
            random.choice(cidades),
            random.uniform(3000, 15000),
            (base_date + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
        ))
    return data

data = generate_sample_data(10000)
df = spark.createDataFrame(
    data,
    ["id", "nome", "departamento", "cidade", "salario", "data_registro"]
)

# Convertendo data
df = df.withColumn("data_registro", col("data_registro").cast("date"))
df = df.withColumn("ano", year("data_registro")) \
       .withColumn("mes", month("data_registro"))

print(f"Total de registros: {df.count()}")
print(f"Partições no DataFrame: {df.rdd.getNumPartitions()}")

# ============================================================================
# REPARTITION vs COALESCE
# ============================================================================

# REPARTITION - Redistribui dados entre partições (shuffle completo)
# Usar quando: aumentar partições ou redistribuir uniformemente
df_repartitioned = df.repartition(10)
print(f"Após repartition(10): {df_repartitioned.rdd.getNumPartitions()} partições")

# Repartition por coluna - dados da mesma coluna ficam na mesma partição
df_by_dept = df.repartition("departamento")
print(f"Repartition por departamento: {df_by_dept.rdd.getNumPartitions()} partições")

# Repartition por coluna com número específico
df_by_dept_n = df.repartition(5, "departamento")

# COALESCE - Reduz partições sem shuffle (mais eficiente)
# Usar quando: diminuir partições antes de escrever
df_coalesced = df.coalesce(2)
print(f"Após coalesce(2): {df_coalesced.rdd.getNumPartitions()} partições")

# ============================================================================
# PARTICIONAMENTO AO ESCREVER - PARTITIONBY
# ============================================================================

output_path = "c:/Users/morae/dev/data-engineering-study/data/output"

# Particionamento simples por uma coluna
df.write \
    .mode("overwrite") \
    .partitionBy("departamento") \
    .parquet(f"{output_path}/por_departamento")

# Particionamento por múltiplas colunas (hierárquico)
# Estrutura: /ano=2023/mes=01/data.parquet
df.write \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .parquet(f"{output_path}/por_data")

# Particionamento com compressão
df.write \
    .mode("overwrite") \
    .partitionBy("departamento") \
    .option("compression", "snappy") \
    .parquet(f"{output_path}/por_dept_comprimido")

# ============================================================================
# BUCKETING - Para JOINs otimizados
# ============================================================================

# Bucketing é útil quando você faz JOINs frequentes em uma coluna específica
# Os dados são pré-organizados em buckets pela coluna especificada

# IMPORTANTE: Bucketing só funciona com saveAsTable (Hive)
# df.write \
#     .mode("overwrite") \
#     .bucketBy(10, "departamento") \
#     .sortBy("salario") \
#     .saveAsTable("funcionarios_bucketed")

# ============================================================================
# PARTITION PRUNING - Otimização automática
# ============================================================================

# Lendo dados particionados - Spark faz partition pruning automaticamente
df_partitioned = spark.read.parquet(f"{output_path}/por_data")

# Esta query só lê as partições necessárias (ano=2023, mes=1)
df_filtered = df_partitioned.filter((col("ano") == 2023) & (col("mes") == 1))
df_filtered.explain(True)  # Veja "PartitionFilters" no plano

# ============================================================================
# ESTRATÉGIAS DE PARTICIONAMENTO
# ============================================================================

"""
ESTRATÉGIAS COMUNS:

1. Por DATA (mais comum para time-series):
   - partitionBy("ano", "mes", "dia") - para dados diários
   - partitionBy("ano", "mes") - para dados mensais
   - partitionBy("data") - cuidado com muitas partições pequenas

2. Por CATEGORIA:
   - partitionBy("regiao", "tipo")
   - Bom quando queries filtram frequentemente por essas colunas

3. Por HASH (implícito com repartition):
   - repartition(n, "coluna") - distribui uniformemente

REGRAS DE OURO:
- Partições não devem ser muito pequenas (< 128MB)
- Partições não devem ser muito grandes (> 1GB)
- Evite colunas com alta cardinalidade (ex: user_id)
- Partição ideal: 128MB - 1GB
- Use partition pruning sempre que possível
"""

# ============================================================================
# CONTROLANDO NÚMERO DE ARQUIVOS POR PARTIÇÃO
# ============================================================================

# Problema: Muitos arquivos pequenos
# Solução: Coalesce antes de escrever

# Escrever com número controlado de arquivos por partição
df.repartition("departamento") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .partitionBy("departamento") \
    .parquet(f"{output_path}/single_file_per_partition")

# Usando maxRecordsPerFile para controlar tamanho
df.write \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 100000) \
    .partitionBy("departamento") \
    .parquet(f"{output_path}/controlled_size")

# ============================================================================
# DYNAMIC PARTITION OVERWRITE
# ============================================================================

# Por padrão, overwrite substitui TODAS as partições
# Com dynamic, só substitui partições que têm novos dados

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Agora só as partições presentes no DF serão substituídas
novos_dados = df.filter(col("departamento") == "TI")
novos_dados.write \
    .mode("overwrite") \
    .partitionBy("departamento") \
    .parquet(f"{output_path}/por_departamento")

# ============================================================================
# ANALISANDO PARTIÇÕES
# ============================================================================

# Ver estrutura das partições
df_read = spark.read.parquet(f"{output_path}/por_departamento")
print("\nPartições existentes:")
df_read.select("departamento").distinct().show()

# Contar registros por partição
df_read.groupBy("departamento").count().show()

# Ver tamanho das partições (em RDD)
def show_partition_sizes(df, name):
    print(f"\n{name}:")
    print(f"Número de partições: {df.rdd.getNumPartitions()}")
    partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    for i, size in enumerate(partition_sizes):
        print(f"  Partição {i}: {size} registros")

show_partition_sizes(df, "Original")
show_partition_sizes(df.repartition(4), "Repartitioned(4)")

# ============================================================================
# SPARK SQL - MSCK REPAIR TABLE (para tabelas Hive)
# ============================================================================

# Quando você adiciona partições manualmente, precisa atualizar o metastore
# spark.sql("MSCK REPAIR TABLE minha_tabela")

# Ou adicionar partição específica
# spark.sql("ALTER TABLE minha_tabela ADD PARTITION (ano=2024, mes=01)")

spark.stop()

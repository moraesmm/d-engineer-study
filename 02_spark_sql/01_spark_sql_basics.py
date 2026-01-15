"""
SparkSQL - Queries SQL no Spark
Tópicos: createOrReplaceTempView, SQL queries, CTEs, subqueries
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ============================================================================
# CRIANDO DADOS DE EXEMPLO
# ============================================================================

funcionarios_data = [
    (1, "João", "Vendas", 5000.0, "2023-01-15", 1),
    (2, "Maria", "TI", 7500.0, "2022-06-20", 2),
    (3, "Pedro", "Vendas", 4500.0, "2023-03-10", 1),
    (4, "Ana", "RH", 6000.0, "2021-11-05", 3),
    (5, "Carlos", "TI", 8000.0, "2020-08-22", 2),
    (6, "Julia", "TI", 6500.0, "2023-07-01", 2),
]

vendas_data = [
    (1, 1, 10000.0, "2024-01-15"),
    (2, 1, 15000.0, "2024-02-20"),
    (3, 3, 8000.0, "2024-01-25"),
    (4, 1, 12000.0, "2024-03-10"),
    (5, 3, 9500.0, "2024-02-28"),
]

departamentos_data = [
    (1, "Vendas", "São Paulo"),
    (2, "TI", "Rio de Janeiro"),
    (3, "RH", "Belo Horizonte"),
]

df_funcionarios = spark.createDataFrame(
    funcionarios_data,
    ["id", "nome", "departamento", "salario", "data_contratacao", "dept_id"]
)

df_vendas = spark.createDataFrame(
    vendas_data,
    ["venda_id", "funcionario_id", "valor", "data_venda"]
)

df_departamentos = spark.createDataFrame(
    departamentos_data,
    ["dept_id", "nome_dept", "cidade"]
)

# ============================================================================
# REGISTRANDO VIEWS TEMPORÁRIAS
# ============================================================================

df_funcionarios.createOrReplaceTempView("funcionarios")
df_vendas.createOrReplaceTempView("vendas")
df_departamentos.createOrReplaceTempView("departamentos")

# View global (visível em todas as sessões)
df_funcionarios.createOrReplaceGlobalTempView("funcionarios_global")
# Acesso: spark.sql("SELECT * FROM global_temp.funcionarios_global")

# ============================================================================
# QUERIES BÁSICAS
# ============================================================================

# SELECT simples
spark.sql("SELECT * FROM funcionarios").show()

# SELECT com filtro
spark.sql("""
    SELECT nome, salario, departamento
    FROM funcionarios
    WHERE salario > 5000
    ORDER BY salario DESC
""").show()

# SELECT com funções
spark.sql("""
    SELECT
        nome,
        salario,
        salario * 12 AS salario_anual,
        UPPER(nome) AS nome_maiusculo,
        YEAR(data_contratacao) AS ano_contratacao
    FROM funcionarios
""").show()

# ============================================================================
# AGREGAÇÕES
# ============================================================================

spark.sql("""
    SELECT
        departamento,
        COUNT(*) AS total_funcionarios,
        SUM(salario) AS soma_salarios,
        AVG(salario) AS media_salario,
        MIN(salario) AS menor_salario,
        MAX(salario) AS maior_salario,
        ROUND(STDDEV(salario), 2) AS desvio_padrao
    FROM funcionarios
    GROUP BY departamento
    HAVING COUNT(*) > 1
    ORDER BY media_salario DESC
""").show()

# ============================================================================
# JOINS
# ============================================================================

# Inner Join
spark.sql("""
    SELECT
        f.nome,
        f.salario,
        d.nome_dept,
        d.cidade
    FROM funcionarios f
    INNER JOIN departamentos d ON f.dept_id = d.dept_id
""").show()

# Left Join com vendas
spark.sql("""
    SELECT
        f.nome,
        f.departamento,
        COALESCE(SUM(v.valor), 0) AS total_vendas,
        COUNT(v.venda_id) AS qtd_vendas
    FROM funcionarios f
    LEFT JOIN vendas v ON f.id = v.funcionario_id
    GROUP BY f.nome, f.departamento
    ORDER BY total_vendas DESC
""").show()

# ============================================================================
# SUBQUERIES
# ============================================================================

# Subquery no WHERE
spark.sql("""
    SELECT nome, salario
    FROM funcionarios
    WHERE salario > (SELECT AVG(salario) FROM funcionarios)
""").show()

# Subquery no FROM
spark.sql("""
    SELECT
        dept_stats.departamento,
        dept_stats.media_salario,
        dept_stats.ranking
    FROM (
        SELECT
            departamento,
            AVG(salario) AS media_salario,
            RANK() OVER (ORDER BY AVG(salario) DESC) AS ranking
        FROM funcionarios
        GROUP BY departamento
    ) dept_stats
""").show()

# Subquery correlacionada
spark.sql("""
    SELECT
        f1.nome,
        f1.departamento,
        f1.salario
    FROM funcionarios f1
    WHERE f1.salario = (
        SELECT MAX(f2.salario)
        FROM funcionarios f2
        WHERE f2.departamento = f1.departamento
    )
""").show()

# ============================================================================
# CTEs (Common Table Expressions)
# ============================================================================

spark.sql("""
    WITH stats_dept AS (
        SELECT
            departamento,
            AVG(salario) AS media_salario,
            COUNT(*) AS total_func
        FROM funcionarios
        GROUP BY departamento
    ),
    vendedores_ativos AS (
        SELECT DISTINCT funcionario_id
        FROM vendas
        WHERE data_venda >= '2024-01-01'
    )
    SELECT
        f.nome,
        f.departamento,
        f.salario,
        sd.media_salario AS media_dept,
        CASE WHEN va.funcionario_id IS NOT NULL THEN 'Sim' ELSE 'Não' END AS vendeu_2024
    FROM funcionarios f
    JOIN stats_dept sd ON f.departamento = sd.departamento
    LEFT JOIN vendedores_ativos va ON f.id = va.funcionario_id
""").show()

# ============================================================================
# WINDOW FUNCTIONS em SQL
# ============================================================================

spark.sql("""
    SELECT
        nome,
        departamento,
        salario,
        ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY salario DESC) AS row_num,
        RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) AS ranking,
        DENSE_RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) AS dense_ranking,
        SUM(salario) OVER (PARTITION BY departamento) AS total_dept,
        AVG(salario) OVER (PARTITION BY departamento) AS media_dept,
        salario - AVG(salario) OVER (PARTITION BY departamento) AS diff_media,
        LEAD(salario, 1) OVER (ORDER BY salario) AS proximo_salario,
        LAG(salario, 1) OVER (ORDER BY salario) AS salario_anterior,
        FIRST_VALUE(nome) OVER (PARTITION BY departamento ORDER BY salario DESC) AS top_salario_nome,
        NTILE(4) OVER (ORDER BY salario) AS quartil
    FROM funcionarios
""").show(truncate=False)

# Running total
spark.sql("""
    SELECT
        v.data_venda,
        v.valor,
        SUM(v.valor) OVER (ORDER BY v.data_venda ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
    FROM vendas v
    ORDER BY v.data_venda
""").show()

# ============================================================================
# CASE WHEN
# ============================================================================

spark.sql("""
    SELECT
        nome,
        salario,
        CASE
            WHEN salario < 5000 THEN 'Junior'
            WHEN salario BETWEEN 5000 AND 7000 THEN 'Pleno'
            ELSE 'Senior'
        END AS nivel,
        CASE departamento
            WHEN 'TI' THEN 'Tecnologia'
            WHEN 'RH' THEN 'Recursos Humanos'
            ELSE departamento
        END AS dept_completo
    FROM funcionarios
""").show()

# ============================================================================
# FUNÇÕES DE DATA
# ============================================================================

spark.sql("""
    SELECT
        nome,
        data_contratacao,
        YEAR(data_contratacao) AS ano,
        MONTH(data_contratacao) AS mes,
        DAY(data_contratacao) AS dia,
        QUARTER(data_contratacao) AS trimestre,
        DATE_FORMAT(data_contratacao, 'dd/MM/yyyy') AS data_br,
        DATEDIFF(CURRENT_DATE(), data_contratacao) AS dias_empresa,
        MONTHS_BETWEEN(CURRENT_DATE(), data_contratacao) AS meses_empresa,
        ADD_MONTHS(data_contratacao, 12) AS um_ano_depois,
        DATE_ADD(data_contratacao, 30) AS mais_30_dias,
        LAST_DAY(data_contratacao) AS ultimo_dia_mes
    FROM funcionarios
""").show()

# ============================================================================
# FUNÇÕES DE STRING
# ============================================================================

spark.sql("""
    SELECT
        nome,
        UPPER(nome) AS maiusculo,
        LOWER(nome) AS minusculo,
        LENGTH(nome) AS tamanho,
        CONCAT(nome, ' - ', departamento) AS nome_dept,
        CONCAT_WS(' | ', nome, departamento, CAST(salario AS STRING)) AS concatenado,
        SUBSTRING(nome, 1, 3) AS primeiras_letras,
        REPLACE(departamento, 'Vendas', 'Sales') AS dept_en,
        TRIM('  texto  ') AS sem_espacos,
        LPAD(CAST(id AS STRING), 5, '0') AS id_formatado
    FROM funcionarios
""").show()

# ============================================================================
# EXISTS e IN
# ============================================================================

# EXISTS
spark.sql("""
    SELECT f.nome, f.departamento
    FROM funcionarios f
    WHERE EXISTS (
        SELECT 1 FROM vendas v WHERE v.funcionario_id = f.id
    )
""").show()

# NOT EXISTS
spark.sql("""
    SELECT f.nome, f.departamento
    FROM funcionarios f
    WHERE NOT EXISTS (
        SELECT 1 FROM vendas v WHERE v.funcionario_id = f.id
    )
""").show()

# IN
spark.sql("""
    SELECT nome, departamento
    FROM funcionarios
    WHERE departamento IN ('TI', 'RH')
""").show()

# ============================================================================
# UNION, INTERSECT, EXCEPT
# ============================================================================

spark.sql("""
    SELECT nome, departamento FROM funcionarios WHERE departamento = 'TI'
    UNION ALL
    SELECT nome, departamento FROM funcionarios WHERE salario > 6000
""").show()

spark.sql("""
    SELECT nome FROM funcionarios WHERE departamento = 'TI'
    INTERSECT
    SELECT nome FROM funcionarios WHERE salario > 6000
""").show()

spark.sql("""
    SELECT nome FROM funcionarios WHERE departamento = 'TI'
    EXCEPT
    SELECT nome FROM funcionarios WHERE salario > 7000
""").show()

# ============================================================================
# CRIANDO TABELAS GERENCIADAS
# ============================================================================

# spark.sql("CREATE DATABASE IF NOT EXISTS estudo")
# spark.sql("USE estudo")

# spark.sql("""
#     CREATE TABLE IF NOT EXISTS funcionarios_managed (
#         id INT,
#         nome STRING,
#         departamento STRING,
#         salario DOUBLE,
#         data_contratacao DATE
#     )
#     USING PARQUET
#     PARTITIONED BY (departamento)
# """)

# ============================================================================
# EXPLAIN - Analisando plano de execução
# ============================================================================

spark.sql("""
    SELECT f.nome, SUM(v.valor) as total
    FROM funcionarios f
    JOIN vendas v ON f.id = v.funcionario_id
    GROUP BY f.nome
""").explain(True)

spark.stop()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def transform(spark, raw_path="data/raw/", processed_path="data/processed/"):
    """Transforma os dados carregados, aplicando limpeza e padronização."""

    os.makedirs(processed_path, exist_ok=True)

    # 🔹 Lendo os DataFrames do raw_path
    dfs = {}
    for file in os.listdir(raw_path):
        file_path = os.path.join(raw_path, file)
        
        if file.endswith(".csv"):
            df = spark.read.option("header", "true").csv(file_path)
            dfs[file] = df
    
    if not dfs:
        print("❌ Nenhum arquivo para transformar.")
        return

    # 🔥 Aplicando transformações
    df_cnaes = dfs.get("cnaes.csv")
    df_est1 = dfs.get("estabelecimentos-1.csv")
    df_est2 = dfs.get("estabelecimentos-2.csv")
    df_est3 = dfs.get("estabelecimentos-3.csv")

    if df_cnaes:
        df_cnaes = df_cnaes.withColumnRenamed("código", "codigo_cnae").withColumnRenamed("descrição", "descricao")

    # 🔥 Unindo os 3 arquivos de estabelecimentos
    df_estabelecimentos = df_est1.union(df_est2).union(df_est3)

    # 🔹 Padronizando colunas
    df_estabelecimentos = df_estabelecimentos.withColumnRenamed("cnpj", "cnpj_basico") \
                                             .withColumnRenamed("nome_fantasia", "nome") \
                                             .withColumnRenamed("cidade", "municipio") \
                                             .withColumnRenamed("uf", "estado")

    # 🔹 Adicionando data de processamento
    df_estabelecimentos = df_estabelecimentos.withColumn("data_processamento", current_timestamp())

    # 🔹 Removendo duplicatas
    df_estabelecimentos = df_estabelecimentos.dropDuplicates(["cnpj_basico"])

    # 🚀 Salvando os resultados
    df_estabelecimentos.write.mode("overwrite").parquet(f"{processed_path}/estabelecimentos.parquet")
    df_cnaes.write.mode("overwrite").parquet(f"{processed_path}/cnaes.parquet")

    print("✅ Dados transformados e salvos!")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL-101 Data Spark").getOrCreate()
    transform(spark)

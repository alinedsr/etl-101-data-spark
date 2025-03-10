import os
from pyspark.sql import SparkSession

# Cria a sessão do Spark
spark = SparkSession.builder \
    .appName("ETL Spark") \
    .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.hadoop.io.nativeio.NativeIO$Windows.access0", "false") \
    .getOrCreate()

def load_data(spark, raw_path="data/raw/", processed_path="data/processed/"):
    """Carrega todos os arquivos CSV/Parquet dentro de qualquer subpasta de raw_path e salva no processed_path."""
    
    # 🔍 Busca arquivos dentro de todas as subpastas
    files = []
    for root, _, filenames in os.walk(raw_path):
        for filename in filenames:
            if filename.endswith((".csv", ".parquet")):
                files.append(os.path.join(root, filename))

    if not files:
        print("❌ Nenhum arquivo válido encontrado para carregar.")
        return
    
    print(f"📂 {len(files)} arquivos encontrados para carregar!")

    # 🚀 Lendo os arquivos com Spark
    dfs = {}
    for file_path in files:
        file_name = os.path.basename(file_path)
        
        if file_name.endswith(".csv"):
            print(f"🟢 Carregando {file_name} como DataFrame Spark (CSV)...")
            df = spark.read.option("header", "true").csv(file_path)
        
        elif file_name.endswith(".parquet"):
            print(f"🟣 Carregando {file_name} como DataFrame Spark (Parquet)...")
            df = spark.read.parquet(file_path)

        dfs[file_name] = df

    # 🚀 Salvando os DataFrames no processed_path como Parquet
    os.makedirs(processed_path, exist_ok=True)  # Garante que a pasta existe

    for file_name, df in dfs.items():
        output_path = os.path.join(processed_path, file_name.replace(".csv", "").replace(".parquet", ""))
        print(f"💾 Salvando {file_name} em {output_path}.parquet ...")
        df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Todos os arquivos foram salvos em {processed_path}")
    return dfs

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL-101 Data Spark").getOrCreate()
    dataframes = load_data(spark)

    for name, df in dataframes.items():
        print(f"✅ {name}: {df.count()} registros carregados.")

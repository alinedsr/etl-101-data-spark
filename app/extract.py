import os
import zipfile
from pyspark.sql import SparkSession

# Caminho do ZIP
ZIP_PATH = "data/raw/dados.zip"  # 🔹 Ajuste o nome do arquivo ZIP
EXTRACT_PATH = "data/raw/"

def get_spark_session():
    """Configura e retorna uma SparkSession"""
    return SparkSession.builder.appName("ETL-Spark").getOrCreate()

def extract_zip(zip_path=ZIP_PATH, extract_path=EXTRACT_PATH):
    """Descompacta o arquivo ZIP e retorna os arquivos extraídos."""
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"❌ Arquivo ZIP não encontrado: {zip_path}")

    os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    print(f"✅ Arquivos extraídos para: {extract_path}")
    return os.listdir(extract_path)

def load_data(spark, extract_path=EXTRACT_PATH):
    """Carrega os arquivos extraídos usando Spark e retorna os DataFrames"""
    files = os.listdir(extract_path)
    dfs = {}

    for file in files:
        file_path = os.path.join(extract_path, file)
        if file.endswith(".csv"):
            print(f"📂 Carregando {file} como DataFrame Spark...")
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            dfs[file] = df
        elif file.endswith(".parquet"):
            print(f"📂 Carregando {file} como DataFrame Spark (Parquet)...")
            df = spark.read.parquet(file_path)
            dfs[file] = df

    return dfs

if __name__ == "__main__":
    spark = get_spark_session()
    extracted_files = extract_zip()
    dataframes = load_data(spark)
    
    # ✅ Listar os DataFrames carregados
    for file, df in dataframes.items():
        print(f"🔹 {file}: {df.count()} registros carregados")
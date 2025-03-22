import os
import shutil
import glob
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, count

# --- Descargar el dataset si no existe ---
def download_dataset():
    if not any(fname.endswith(".csv") for fname in os.listdir("data")):
        print("📥 Descargando dataset desde Kaggle...")
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('elemento/nyc-yellow-taxi-trip-data', path='data', unzip=True)
    else:
        print("✅ Dataset ya disponible.")

# --- Crear carpeta results (limpia) ---
def prepare_results_dir():
    os.makedirs("results", exist_ok=True)
    for f in os.listdir("results"):
        os.remove(os.path.join("results", f))

# --- Procesar con Spark ---
def process_with_spark():
    spark = SparkSession.builder.appName("TaxiDataProcessing").getOrCreate()

    # Buscar el primer archivo .csv
    csv_files = glob.glob("data/*.csv")
    if not csv_files:
        raise FileNotFoundError("❌ No se encontró ningún archivo .csv en 'data/'.")

    dataset_path = csv_files[0]
    print(f"📂 Procesando archivo: {dataset_path}")

    df = spark.read.csv(dataset_path, header=True, inferSchema=True)

    # Selección de columnas clave (pueden variar según año)
    columnas = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]
    columnas_presentes = [col for col in columnas if col in df.columns]
    if len(columnas_presentes) < 4:
        raise Exception("❌ El archivo CSV no contiene todas las columnas necesarias.")

    df_filtered = df.select(*columnas_presentes)

    # Guardar muestra
    df_filtered.limit(1000).toPandas().to_json("results/data.json", orient="records", indent=2)

    # Crear resumen por hora
    df_summary = df_filtered.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .groupBy("pickup_hour") \
        .agg(
            avg("trip_distance").alias("avg_distance"),
            avg("total_amount").alias("avg_amount"),
            count("*").alias("total_trips")
        )

    df_summary.toPandas().to_json("results/summary.json", orient="records", indent=2)

    print("✅ Archivos generados: results/data.json y results/summary.json")

# --- Ejecutar todo ---
if __name__ == "__main__":
    download_dataset()
    prepare_results_dir()
    process_with_spark()
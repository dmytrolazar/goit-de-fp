# landing_to_bronze.py
from pyspark.sql import SparkSession
import requests
from pyspark.sql import SparkSession

def download_csv_to_dbfs(ftp_url, save_path):
    """
    Завантажує CSV-файл із FTP-сервера до DBFS (Databricks File System).
    
    :param ftp_url: URL до файлу на FTP-сервері
    :param save_path: Шлях у DBFS для збереження файлу
    """
    print(f"Завантаження CSV-файлу з FTP-сервера: {ftp_url}")
    response = requests.get(ftp_url)
    
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Файл успішно завантажено до локального шляху: {save_path}")
        
def process_csv_with_spark(spark, dbfs_path, save_path):
    """
    Обробляє CSV-файл за допомогою Spark і зберігає його у форматі Parquet.
    
    :param spark: SparkSession
    :param dbfs_path: Шлях до CSV-файлу у DBFS
    :param save_path: Шлях для збереження даних у форматі Parquet
    """
    print(f"Читання CSV-файлу з DBFS: {dbfs_path}")
    
    # Читання CSV-файлу за допомогою Spark
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(dbfs_path)
    
    print(f"Файл успішно оброблено. Розмір DataFrame: {df.count()} рядків")
    
    # Збереження у форматі Parquet
    print(f"Збереження даних у Parquet за шляхом: {save_path}")
    df.write.parquet(save_path, mode="overwrite")
    print("Дані успішно збережені.")



# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("FTP_to_Bronze") \
    .getOrCreate()

# Шляхи до файлів на FTP-сервері
ftp_path = "https://ftp.goit.study/neoversity"
ftp_athlete_bio_url = f"{ftp_path}/athlete_bio.csv"
ftp_athlete_event_results_url = f"{ftp_path}/athlete_event_results.csv"

# Шляхи для тимчасового збереження у DBFS
dbfs_athlete_bio_path = "/tmp/athlete_bio.csv"
dbfs_athlete_event_results_path = "/tmp/athlete_event_results.csv"

# Шляхи для збереження у Bronze рівень
bronze_athlete_bio_path = "/mnt/bronze/athlete_bio"
bronze_athlete_event_results_path = "/mnt/bronze/athlete_event_results"

# Завантаження та обробка athlete_bio
print("Початок завантаження athlete_bio...")
download_csv_to_dbfs(ftp_athlete_bio_url, dbfs_athlete_bio_path)
athlete_bio_df = spark.read.format("csv").option("header", "true").load(f"file:{dbfs_athlete_bio_path}")

athlete_bio_df.show()

athlete_bio_df.write.parquet(bronze_athlete_bio_path, mode="overwrite")


# Завантаження та обробка athlete_event
print("Початок завантаження athlete_event_results...")
download_csv_to_dbfs(ftp_athlete_event_results_url, dbfs_athlete_event_results_path)
athlete_event_results_df = spark.read.format("csv").option("header", "true").load(f"file:{dbfs_athlete_event_results_path}")

athlete_event_results_df.show()

athlete_event_results_df.write.parquet(bronze_athlete_event_results_path, mode="overwrite")

print(f"Data saved to Bronze layer at /mnt/bronze/")

# spark.stop()


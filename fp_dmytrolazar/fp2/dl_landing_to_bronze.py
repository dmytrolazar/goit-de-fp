import requests
from pyspark.sql import SparkSession

def download_csv(ftp_url, save_path):
    print(f"Завантаження CSV-файлу з FTP-сервера: {ftp_url}")
    response = requests.get(ftp_url)
    
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Файл успішно завантажено до локального шляху: {save_path}")
        
def process_csv_with_spark(spark, path, save_path):
    print(f"Читання CSV-файлу з файлової системи: {path}")
    
    # Читання CSV-файлу за допомогою Spark
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(path)
    
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

# Шляхи для тимчасового збереження у файловій системі
athlete_bio_path = "/tmp/athlete_bio.csv"
athlete_event_results_path = "/tmp/athlete_event_results.csv"

# Шляхи для збереження у Bronze рівень
bronze_athlete_bio_path = "/mnt/bronze/athlete_bio"
bronze_athlete_event_results_path = "/mnt/bronze/athlete_event_results"

# Завантаження та обробка athlete_bio
print("Початок завантаження athlete_bio...")
download_csv(ftp_athlete_bio_url, athlete_bio_path)
athlete_bio_df = spark.read.format("csv").option("header", "true").load(f"file:{athlete_bio_path}")

athlete_bio_df.show()

athlete_bio_df.write.parquet(bronze_athlete_bio_path, mode="overwrite")


# Завантаження та обробка athlete_event
print("Початок завантаження athlete_event_results...")
download_csv(ftp_athlete_event_results_url, athlete_event_results_path)
athlete_event_results_df = spark.read.format("csv").option("header", "true").load(f"file:{athlete_event_results_path}")

athlete_event_results_df.show()

athlete_event_results_df.write.parquet(bronze_athlete_event_results_path, mode="overwrite")

print(f"Data saved to Bronze layer at /mnt/bronze/")

# spark.stop()


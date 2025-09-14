#silver_to_gold.py
from pyspark.sql.functions import col, current_timestamp, avg
from pyspark.sql import SparkSession

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("Silver_to_Gold") \
    .getOrCreate()


# Шляхи до Silver рівня
silver_bio_path = "/mnt/silver/athlete_bio"
silver_event_path = "/mnt/silver/athlete_event_results"

# Читання даних із Silver рівня
print("Читання даних із Silver рівня...")
bio_df = spark.read.parquet(silver_bio_path)
event_df = spark.read.parquet(silver_event_path)

# Перейменування колонок для уникнення конфліктів
event_df = event_df.withColumnRenamed("country_noc", "event_country_noc")

# Об'єднання таблиць за athlete_id
print("Об'єднання даних...")
joined_df = bio_df.join(event_df, on="athlete_id", how="inner")

# Приведення типів даних
print("Перетворення типів даних...")
joined_df = joined_df.withColumn("height", col("height").cast("Double")) \
                     .withColumn("weight", col("weight").cast("Double"))

# Обчислення середніх значень weight і height для кожної комбінації sport, medal, sex, country_noc
print("Обчислення середніх значень...")
avg_stats_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    )

# Додавання стовпця timestamp
print("Додавання стовпця timestamp...")
avg_stats_df = avg_stats_df.withColumn("timestamp", current_timestamp())

avg_stats_df.show()

# Збереження результатів у Gold рівень
gold_avg_stats_path = "/mnt/gold/avg_stats"
print(f"Збереження даних у Gold рівень за шляхом: {gold_avg_stats_path}")
avg_stats_df.write.parquet(gold_avg_stats_path, mode="overwrite")
print("Дані успішно збережені у Gold рівень.")

# spark.stop()
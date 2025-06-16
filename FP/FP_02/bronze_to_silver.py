# bronze_to_silver.py
def bronze_to_silver(table_name):
    print(f"Читання даних із Bronze рівня {table_name}...")
    bronze_path = f"/mnt/bronze/{table_name}"

    try:
        df = spark.read.parquet(bronze_path)
        print("Читання завершено. Розмір DataFrame:")
        print(f"Кількість рядків: {df.count()}, Кількість колонок: {len(df.columns)}")
    except Exception as e:
        print(f"Помилка при читанні даних: {e}")


    import re
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    # Функція очищення тексту
    def clean_text(text):
        # Видаляє всі символи, крім букв, цифр, ком, крапок та лапок
        return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

    # Створення UDF для використання у Spark
    clean_text_udf = udf(clean_text, StringType())

    # Очищення текстових колонок
    print("Очищення текстових колонок...")
    text_columns = [col_name for col_name, dtype in df.dtypes if dtype == "string"]

    try:
        for col_name in text_columns:
            print(f"Очищення колонки: {col_name}")
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))
        print("Очищення завершено.")
    except Exception as e:
        print(f"Помилка при очищенні текстових колонок: {e}")

    # Видалення дублікатів
    print("Видалення дублікатів...")
    try:
        print(f"Кількість рядків до видалення дублікатів: {df.count()}")

        df = df.dropDuplicates()
        print("Видалення дублікатів завершено.")
        print(f"Кількість рядків після видалення дублікатів: {df.count()}")
    except Exception as e:
        print(f"Помилка при видаленні дублікатів: {e}")


    df.show()

    # Збереження даних у Silver рівень
    print(f"Збереження даних {table_name} у Silver рівень...")
    silver_path = f"/mnt/silver/{table_name}"

    try:
        df.write.parquet(silver_path, mode="overwrite")
        print(f"Дані збережено у Silver рівень за шляхом: {silver_path}")
    except Exception as e:
        print(f"Помилка при збереженні даних: {e}")

from pyspark.sql import SparkSession


# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("Bronze_to_Silver") \
    .getOrCreate()

bronze_to_silver("athlete_bio")
bronze_to_silver("athlete_event_results")

# spark.stop()
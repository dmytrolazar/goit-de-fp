# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio (база даних і Credentials до неї вам будуть надані).

# from pyspark.sql import SparkSession

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"

jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# spark.conf.set("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.32")
# not working in databricks community edition

# встановити в databricks кластер вручну 2 бібліотеки (maven):
# com.mysql:mysql-connector-j:8.0.32
# org.apache.kafka:kafka-clients:3.2.3

# Читання даних з SQL бази даних

athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",  # com.mysql.jdbc.Driver
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

# display(athlete_bio_df)




# COMMAND ----------

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами. Можна це зробити на будь-якому етапі вашої програми.

athlete_bio_filtered_df = (
    athlete_bio_df.filter(
        (F.col("height").cast("double").isNotNull())
        & (F.col("weight").cast("double").isNotNull())
    )
)

# display(athlete_bio_filtered_df)



# COMMAND ----------

# MAGIC %run ./kafka_utils

# COMMAND ----------

# 3.1 Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results. 

# читаємо з mysql
athlete_results_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",  # com.mysql.jdbc.Driver
        dbtable="athlete_event_results",
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

# display(athlete_results_df)




# COMMAND ----------

# імітуємо сорс для стрім пайплайна (пишемо івенти у кафку, яка буде сорсом для нашого стрім-пайплайна)

athlete_results_json = athlete_results_df.selectExpr("athlete_id", "to_json(struct(*)) AS value")

# display(athlete_results_json)

topic_name = f'{my_name}_athlete_event_results_fp'

#print(kafka_config)

(
athlete_results_json
    .write.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                      f'username="{kafka_config["username"]}" '
                                      f'password="{kafka_config["password"]}";')
    .option("topic", topic_name)
    .save()
)

# COMMAND ----------

# 3.2 Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

# from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Пакет, необхідний для читання Kafka зі Spark
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


# додати в databricks кластер вручну бібліотеку, якщо ще не додали (maven):
# org.apache.kafka:kafka-clients:3.2.3

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
topic_name = f'{my_name}_athlete_event_results_fp'

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")


# Обробка даних Kafka
# Декодування значень (Kafka надсилає повідомлення як байтові масиви)
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

sample_json = '{"edition":"1908SummerOlympics","edition_id":5,"country_noc":"ANZ","sport":"Athletics","event":"100metres,Men","result_id":56265,"athlete":"ErnestHutcheon","athlete_id":64710,"pos":"DNS","medal":"nan","isTeamSport":"False"}'

schema = schema_of_json(sample_json)

# 5. Конвертація JSON у DataFrame
athlete_results_from_kafka_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# display(athlete_results_from_kafka_df)




# COMMAND ----------

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.

joined_df = athlete_results_from_kafka_df.join(athlete_bio_filtered_df.withColumnRenamed("country_noc", "bio_country_noc"), on="athlete_id", how="inner")

# display(joined_df)

# COMMAND ----------

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.

from pyspark.sql.functions import avg, current_timestamp

# Групування за потрібними категоріями та обчислення середнього значення
result_df = joined_df.groupBy(
    "sport",       # Вид спорту
    "medal",       # Тип медалі або її відсутність
    "sex",         # Стать
    "country_noc"  # Країна
).agg(
    avg("height").alias("average_height"),  # Середній зріст
    avg("weight").alias("average_weight")  # Середня вага
).withColumn(
    "calculation_timestamp", current_timestamp()  # Додавання часу розрахунків
)

# display(result_df)


# COMMAND ----------

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
    
# а) вихідний кафка-топік,
def write_to_kafka(batch_df, batch_id):
    (
    batch_df.selectExpr("to_json(struct(*)) AS value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                        f'username="{kafka_config["username"]}" '
                                        f'password="{kafka_config["password"]}";')
        .option("topic", f'{my_name}_athlete_enriched_agg')
        .save()
    )


# b) базу даних.
def write_to_database(batch_df, batch_id):
    # # Налаштування конфігурації SQL бази даних
    jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
    jdbc_table = f'{my_name}_athlete_enriched_agg'

    jdbc_user = "neo_data_admin"
    jdbc_password = "Proyahaxuqithab9oplp"

    # Запис даних в SQL базу даних
    
    try:
        batch_df.write \
            .mode("append") \
            .format("jdbc") \
            .options(
                url=jdbc_url,
                driver="com.mysql.cj.jdbc.Driver",  # com.mysql.jdbc.Driver
                dbtable=jdbc_table,
                user=jdbc_user,
                password=jdbc_password,
            ) \
            .save()

        print(f"Batch: {batch_df.show()}")
        print(f"Batch written successfully to table {jdbc_table}.")            
    except Exception as e:
        print(f"Error writing batch to MySQL: {e}")
    



# Функція для запису в два сінка
def write_to_sinks(batch_df, batch_id):
    write_to_kafka(batch_df, batch_id)     # Write batch to Kafka
    write_to_database(batch_df, batch_id)  # Write batch to database

# Налаштування і запуск потоку даних для обробки кожної партії за допомогою вказаної функції
query = result_df.writeStream \
    .foreachBatch(write_to_sinks) \
    .outputMode("update") \
    .start() \
    .awaitTermination()



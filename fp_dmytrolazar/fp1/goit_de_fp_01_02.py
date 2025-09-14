from pyspark.sql import SparkSession, functions as F
from configs import kafka_config, my_name
from pyspark.sql.functions import col, from_json, schema_of_json, avg, current_timestamp
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "16")

spark.sparkContext.setLogLevel("ERROR")

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio

athlete_bio_df = (spark.read.format('jdbc')
    .options(
        url=jdbc_url,
        driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
        dbtable="athlete_bio",
        user=jdbc_user,
        password=jdbc_password,
        partitionColumn="athlete_id",
        lowerBound=1,
        upperBound=22000000,
        numPartitions="10",
    )
    .load())

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами

athlete_bio_filtered_df = (
    athlete_bio_df.filter(
        (F.col("height").cast("double").isNotNull())
        & (F.col("weight").cast("double").isNotNull())
    )
)
athlete_bio_filtered_df.show(5)

# 3.1 Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results.

athlete_results_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver=jdbc_driver,  # com.mysql.jdbc.Driver
        dbtable="athlete_event_results",
        user=jdbc_user,
        password=jdbc_password,
        partitionColumn="athlete_id",
        lowerBound=1,
        upperBound=90016770,
        numPartitions="10",
    )
    .load()
)
athlete_results_df.show(5)

athlete_results_json = athlete_results_df.selectExpr("athlete_id", "to_json(struct(*)) AS value")

topic_name = f'{my_name}_athlete_event_results_fp'

(athlete_results_json
    .write.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                      f'username="{kafka_config["username"]}" '
                                      f'password="{kafka_config["password"]}";')
    .option("topic", topic_name)
    .save()
)

# 3.2 Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                      f'username="{kafka_config["username"]}" '
                                      f'password="{kafka_config["password"]}";') \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_value")

# 3.3 Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.

sample_json = '{"edition":"1908SummerOlympics","edition_id":5,"country_noc":"ANZ","sport":"Athletics","event":"100metres,Men","result_id":56265,"athlete":"ErnestHutcheon","athlete_id":64710,"pos":"DNS","medal":"nan","isTeamSport":"False"}'

schema = schema_of_json(sample_json)

athlete_results_from_kafka_df = kafka_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

print("done reading")

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.

# from pyspark.sql.functions import broadcast

joined_df = athlete_results_from_kafka_df.join(
    athlete_bio_filtered_df.withColumnRenamed("country_noc", "bio_country_noc"),
    on="athlete_id", how="inner")

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.

result_df = (joined_df
    .repartition("sport", "sex", "country_noc", "medal")
    .groupBy(
        "sport",       # Вид спорту
        "sex",         # Стать
        "country_noc", # Країна
        "medal",       # Тип медалі або її відсутність
    ).agg(
        avg("height").alias("average_height"), # Середній зріст
        avg("weight").alias("average_weight")  # Середня вага
    ).withColumn(
        "calculation_timestamp", current_timestamp()  # Додавання часу розрахунків
    )
)

# 6. Зробити стрим даних (за допомогою функції forEachBatch) у:

# а) вихідний кафка-топік,
def write_to_kafka(batch_df):
    (
    batch_df.selectExpr("to_json(struct(*)) AS value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                                        f'username="{kafka_config["username"]}" '
                                        f'password="{kafka_config["password"]}";')
        .option("topic", f'{my_name}_athlete_enriched_agg')
        .save()
    )


# b) базу даних.
def write_to_database(batch_df):
    jdbc_table = f'athlete_enriched_agg_dmytrolazar'

    try:
        batch_df.write \
            .mode("append") \
            .format("jdbc") \
            .options(
                url=jdbc_url,
                driver=jdbc_driver,
                dbtable=jdbc_table,
                user=jdbc_user,
                password=jdbc_password,
            ) \
            .option("batchsize", 100) \
            .option("isolationLevel", "NONE") \
            .save()

        print(f"Batch written successfully to table {jdbc_table}.")            
    except Exception as e:
        print(f"Error writing batch to MySQL: {e}")
    

# Функція для запису у два приймачі
def write_to_targets(batch_df, batch_id):
    write_to_kafka(batch_df)     # запис у Kafka
    write_to_database(batch_df)  # запис у базу даних

# Запуск потоку даних для обробки кожної партії за допомогою вказаної функції
query = result_df.writeStream \
    .foreachBatch(write_to_targets) \
    .outputMode("update") \
    .start() \
    .awaitTermination()

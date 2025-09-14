from kafka import KafkaConsumer
from configs import kafka_config, my_name
       
# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'][0],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='earliest',   # Зчитування повідомлень з початку
    enable_auto_commit=True,        # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3', # Ідентифікатор групи споживачів
    fetch_max_bytes=50 * 1024 * 1024,          # 50 MB per fetch
    max_partition_fetch_bytes=50 * 1024 * 1024 # per partition
)

topic_name = f'{my_name}_athlete_enriched_agg'

# Підписка на тему
consumer.subscribe([topic_name])
print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()


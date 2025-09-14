from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, my_name

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'][0],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

admin_client.delete_topics([t for t in [f'{my_name}_athlete_event_results_fp', f'{my_name}_athlete_enriched_agg'] if t in admin_client.list_topics()])

def create_topic(topic_name):
    num_partitions = 2
    replication_factor = 1

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

create_topic(f'{my_name}_athlete_event_results_fp')
create_topic(f'{my_name}_athlete_enriched_agg')

admin_client.close()



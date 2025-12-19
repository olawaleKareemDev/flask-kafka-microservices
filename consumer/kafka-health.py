import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")

print(f"Waiting for Kafka at {BOOTSTRAP}...")

while True:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            client_id="startup-check",
        )

        existing_topics = admin.list_topics()
        if TOPIC not in existing_topics:
            print(f"Creating topic: {TOPIC}")
            admin.create_topics(
                new_topics=[
                    NewTopic(
                        name=TOPIC,
                        num_partitions=1,
                        replication_factor=1,
                    )
                ],
                validate_only=False,
            )
        else:
            print(f"Topic '{TOPIC}' already exists")

        admin.close()
        break

    except NoBrokersAvailable:
        print("Kafka not ready yet, retrying...")
        time.sleep(3)

    except TopicAlreadyExistsError:
        break

print("Kafka is ready ✅")

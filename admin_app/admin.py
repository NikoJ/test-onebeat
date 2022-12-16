import os
import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, NodeNotReadyError
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)

HOST = os.environ.get("HOST_BROKER")
PORT = os.environ.get("PORT_BROKER")
KAFKA_BROKERS = f"{HOST}:{PORT}"
KAFKA_TOPIC_IN = "demo_in"
KAFKA_TOPIC_OUT = "demo_out"


def main():
    """
    Create a kafka topic
    """
    tries = 3
    for i in range(tries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
            try:
                topic_in = NewTopic(
                    name=KAFKA_TOPIC_IN, num_partitions=1, replication_factor=1
                )
                topic_out = NewTopic(
                    name=KAFKA_TOPIC_OUT, num_partitions=1, replication_factor=1
                )
                admin.create_topics(new_topics=[topic_in, topic_out])
                logging.info("Created topic")
                break
            except TopicAlreadyExistsError as e:
                logging.warning("Topic already exists")
                break
            finally:
                admin.close()
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            if tries > 0:
                tries -= 1
                logging.warning(f"Kafka not ready yet. Tries remaining: {tries}")
                time.sleep(3)
            else:
                logging.error(e)


if __name__ == "__main__":
    main()

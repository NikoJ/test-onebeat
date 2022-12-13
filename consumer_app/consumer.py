import os
from json import loads
from kafka import KafkaConsumer
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO
)

HOST = os.environ.get("HOST_BROKER")
PORT = os.environ.get("PORT_BROKER")
KAFKA_BROKERS = f"{HOST}:{PORT}"
KAFKA_TOPIC_OUT = "demo_out"
ENCODING = 'UTF-8'

def main():
  consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_BROKERS],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=1000, 
    group_id="my_consumer_out",
    value_deserializer=lambda x: loads(x.decode(ENCODING))
  )
  consumer.subscribe(KAFKA_TOPIC_OUT) 

  while True:
    msg_pack = consumer.poll(timeout_ms=500)
    for tp, messages in msg_pack.items():
        for message in messages:
            logging.info(f"Result: {message.value}")

if __name__ == "__main__":
  main()
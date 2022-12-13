import os
import time
from json import dumps
from datetime import date
from kafka import KafkaProducer
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO
)

HOST = os.environ.get("HOST_BROKER")
PORT = os.environ.get("PORT_BROKER")
KAFKA_BROKERS = f"{HOST}:{PORT}" 
KAFKA_TOPIC_IN = "demo_in"

def on_success(metadata):
  logging.info(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  logging.error(f"Error sending message: {e}")

def main():
  producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS
    )
  # Produce asynchronously with callbacks
  future = producer.send(
    KAFKA_TOPIC_IN,
    key=str.encode("date"),
    value=str.encode(str(date.today()))
    )
  future.add_callback(on_success)
  future.add_errback(on_error)
  producer.flush()

if __name__ == "__main__":
    main()
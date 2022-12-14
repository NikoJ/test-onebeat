import os
import json
from kafka import KafkaConsumer, KafkaProducer
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
KAFKA_TOPIC_IN = "demo_in"
ENCODING = 'UTF-8'

def main():
  consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_BROKERS],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=1000,
    group_id="my_consumer_in",
    value_deserializer=lambda x: json.loads(x.decode(ENCODING))
  )

  producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v:json.dumps(v).encode(ENCODING)
    )

  consumer.subscribe(KAFKA_TOPIC_IN) 

  for message in consumer:
    value = message.value["date"]
    sum_for_day = sum_of_digits(value)
    message_info = {"date": value, "sum": sum_for_day}

    future = producer.send(
      KAFKA_TOPIC_OUT,
      value=message_info
      )
    future.add_callback(on_success)
    future.add_errback(on_error)
    producer.flush()

def sum_of_digits(value:str):
  num = int(value.replace("-", ""))
  sum = 0
  while num > 0:
    sum += num % 10
    num //= 10
  return str(sum)

def on_success(metadata):
  logging.info(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  logging.error(f"Error sending message: {e}")

if __name__ == "__main__":
  main()
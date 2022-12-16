FROM custom_python:latest

WORKDIR /app

COPY consumer_producer.py consumer_producer.py

ENTRYPOINT sleep 20 && python3 consumer_producer.py
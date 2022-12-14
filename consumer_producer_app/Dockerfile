FROM python:3.8

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

COPY consumer_producer.py consumer_producer.py

ENTRYPOINT sleep 20 && python3 consumer_producer.py
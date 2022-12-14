FROM python:3.8

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

COPY producer.py producer.py

ENTRYPOINT sleep 10 && python3 producer.py
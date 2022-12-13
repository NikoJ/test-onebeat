FROM python:3.8

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

COPY admin.py admin.py

ENTRYPOINT sleep 5 && python3 admin.py
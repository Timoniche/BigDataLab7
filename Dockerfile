FROM python:3.9-slim

ENV PYTHONUNBUFFERED 1

WORKDIR /app

ADD . /app

ENV PYTHONPATH "${PYTHONPATH}:/app"

RUN apt-get update && apt-get install -y gcc python3-dev

RUN pip install --upgrade pip

RUN pip install -r requirements.txt
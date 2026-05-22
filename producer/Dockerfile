FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py kafka_producer.py main.py ws_client.py ./

CMD ["python", "main.py"]

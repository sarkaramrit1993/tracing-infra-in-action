FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY checkout.py agent.py consumer_clickhouse.py consumer_jaeger.py scatter_gather_query.py ./

EXPOSE 8080

CMD ["python", "checkout.py"]

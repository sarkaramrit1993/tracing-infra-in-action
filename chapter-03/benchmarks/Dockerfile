FROM ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.154.0 AS telemetrygen

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy telemetrygen binary from the contrib image
COPY --from=telemetrygen /telemetrygen /usr/local/bin/telemetrygen

COPY . .

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "runner.py"]

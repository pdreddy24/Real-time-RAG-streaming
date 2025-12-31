# syntax=docker/dockerfile:1

FROM python:3.11-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    librdkafka-dev \
  && rm -rf /var/lib/apt/lists/*

# If you have streaming/requirements.txt, copy it instead.
# For now: minimal deps needed for producer + kafka + SSE.
RUN pip install --upgrade pip \
 && pip wheel --wheel-dir=/wheels \
    confluent-kafka sseclient-py requests


FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* \
 && rm -rf /wheels

COPY . /app

# Run your producer
CMD ["python", "-u", "/app/producer_wikimedia.py"]

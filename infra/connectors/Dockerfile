# syntax=docker/dockerfile:1

FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# If you have a connectors/requirements.txt, use it.
# Otherwise keep it minimal for now:
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir sseclient-py requests

COPY . /app

# Example entry; override in compose if needed
CMD ["python", "-u", "-m", "connectors.wikimedia_sse_connector"]

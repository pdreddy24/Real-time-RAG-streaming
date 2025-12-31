# syntax=docker/dockerfile:1

FROM python:3.11-slim AS base
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY rag/api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

COPY rag /app/rag

FROM base AS api
EXPOSE 8000
CMD ["uvicorn", "rag.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS producer
CMD ["python", "-m", "rag.producer"]

FROM base AS indexer
EXPOSE 9100
CMD ["python", "-m", "rag.indexer"]

FROM base AS ui
RUN pip install --no-cache-dir streamlit
EXPOSE 8501
CMD ["streamlit", "run", "/app/rag/ui/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]

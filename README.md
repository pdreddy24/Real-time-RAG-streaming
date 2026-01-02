##  REAL-TIME STREAMING RAG PIPELINE

## Summary
**Goal:** Keep RAG fresh when knowledge updates every minute.  
**Approach:** Treat RAG as a **streaming data product**:
- **Durable:** broker-backed ingestion
- **Replayable:** offset-based reprocessing
- **Queryable:** vector + metadata retrieval
- **Observable:** lag + latency + error-rate dashboards
- **Demo-ready:** API + UI + provenance

## High-level architecture
<img width="5531" height="1772" alt="wikimedia_streaming_architecture - Copy" src="https://github.com/user-attachments/assets/151458cc-da96-4671-a56f-272db50c3a8f" />

```text
Wikimedia SSE
   │
   ▼
Ingestion (SSE Consumer)
   │
   ▼
Messaging (Redpanda/Kafka topic)
   │
   ▼
Spark Structured Streaming
   │
   ├── Bronze (raw, append-only)
   ├── Silver (cleaned + validated)
   └── Gold (AI-ready docs)
             │
             ▼
Embeddings + pgvector (Postgres)
             │
             ▼
FastAPI Retrieval API (/search, /answer)
             │
             ▼
UI (live feed + answer + citations)
             │
             ▼
Prometheus + Grafana (lag, latency, errors)
````

## Data source
- **Source:** Wikimedia “Recent Changes” via Server-Sent Events (SSE)
- **Nature:** semi-structured, inconsistent, continuous, high-velocity
- **Why SSE:** always-on stream with minimal latency (no polling)
  
## Tech stack

**Streaming + backbone**

  - SSE ingestion client (Python)

  - Redpanda (Kafka-compatible) or Kafka for durability + replay

  - Spark Structured Streaming for continuous transformation

**Storage + RAG**

  - Medallion storage: Parquet/Delta-style tables (local or object storage)

  - Postgres + pgvector for semantic search + metadata filtering

  - LLM provider (e.g., GPT-4 class model) for synthesis (pluggable)

**Serving + product**

  - FastAPI async API layer

  - Custom UI to prove “real-time” freshness + show provenance

**Observability**

  - Prometheus metrics scraping

  - Grafana dashboards + alerting

## Data modeling
**Bronze (raw)**

  - Store events as-is with ingestion time + broker offsets

  - **Purpose:** audit trail, replay, forensics

**Silver (cleaned + validated)**

  - Normalize messy fields, enforce schema, dedupe, drop invalid junk

  - **Purpose:** trusted operational dataset

**Gold (AI-ready)**

**_Curate “documents” for embedding/indexing:_**

  - ```text doc_id, text, event_time, page/title, url, lang, quality_flags, etc. ```

  - **Purpose:** stable retrieval units for RAG
**Why Medallion:** Because RAG without lineage is a liability. When a response is wrong, you need to pinpoint exactly what event/transform caused it—fast.

## API contract
A clean service contract so other apps can consume this like a real platform.

**Endpoints**

```text GET /health``` — service + dependencies health

```text POST /search``` — retrieval-only (vector + filters)

```text POST /answer``` — RAG response with citations + debug metadata

```text GET /events/recent``` — recent cleaned events for UI “freshness proof” (poll)

```text GET /stream ```— optional SSE/WebSocket stream for UI

## Observability (metrics-first)

In streaming systems, “it runs” is not a status. You must be able to answer:

  - Are we falling behind?

  - How long until an edit is searchable?

  - Are we dropping/poisoning data?

**Key metrics**
   - Consumer lag (broker offsets behind head)
   - End-to-end latency: event_time → indexed_time → searchable_time
   - Throughput: events/sec, docs/sec
   - Error rates: parsing failures, embedding failures, API 5xx
   - Query latency: p50/p95/p99 for vector search + LLM calls

**Observability**

  In streaming systems, “it runs” is not a status. You must be able to answer:

  - Are we falling behind?

  - How long until an edit is searchable?

  - Are we dropping/poisoning data?

## Key metrics

**Consumer lag**

**End-to-end latency:** event_time → indexed_time → searchable_time

**Throughput:** events/sec, docs/sec

**Error rates:** parsing failures, embedding failures, API 5xx

**Query latency:** p50/p95/p99 for vector search + LLM calls


<img width="2218" height="1210" alt="Screenshot 2025-12-30 120849" src="https://github.com/user-attachments/assets/2dce105d-151b-4a38-b907-6407baf919a0" />

<img width="1778" height="984" alt="Screenshot 2025-12-30 190911" src="https://github.com/user-attachments/assets/a75a1463-9e8b-471d-9085-4ad6297f4b9a" />

<img width="2239" height="1216" alt="Screenshot 2025-12-30 212737" src="https://github.com/user-attachments/assets/1618fc84-7ace-4aae-ad2c-797e393ee933" />

<img width="2239" height="991" alt="Screenshot 2025-12-30 212755" src="https://github.com/user-attachments/assets/f1c773a3-c099-4d10-80a3-ea7a0c0d6936" />

<img width="1716" height="530" alt="Screenshot 2025-12-30 120727" src="https://github.com/user-attachments/assets/3ee62e17-f88c-4c85-bda7-38d1dc7954ce" />

## Repository layout
```text
├── alertmanager
│   └── alertmanager.yml
├── app
│   ├── rag
│   │   ├── api
│   │   │   ├── __init__.py
│   │   │   ├── app.py
│   │   │   ├── config.py
│   │   │   ├── db.py
│   │   │   ├── Dockerfile.api
│   │   │   ├── embedder.py
│   │   │   ├── main.py
│   │   │   ├── rag.py
│   │   │   └── requirements.txt
│   │   ├── common
│   │   │   └── embedder.py
│   │   ├── db
│   │   │   ├── bootstrap.py
│   │   │   ├── data_model.py
│   │   │   └── schema.sql
│   │   ├── indexer
│   │   │   ├── __init__.py
│   │   │   ├── __main__.py
│   │   │   ├── chunker.py
│   │   │   ├── config.py
│   │   │   ├── consumer.py
│   │   │   ├── db.py
│   │   │   ├── Dockerfile.indexer
│   │   │   ├── embedder.py
│   │   │   ├── indexer.py
│   │   │   ├── metrics.py
│   │   │   ├── metrics_server.py
│   │   │   ├── modeling.py
│   │   │   └── requirements.txt
│   │   ├── observability
│   │   │   ├── metrics_fastapi.py
│   │   │   └── metrics_indexer.py
│   │   ├── producer
│   │   │   ├── __init__.py
│   │   │   ├── __main__.py
│   │   │   ├── Dockerfile.producer
│   │   │   ├── requirements.txt
│   │   │   └── wikimedia_sse_producer.py
│   │   ├── schemas
│   │   │   ├── __init__.py
│   │   │   ├── 05_pgvector.sql
│   │   │   ├── api.py
│   │   │   ├── phase6.py
│   │   │   └── raw_event_wikimedia_v1.json
│   │   ├── ui
│   │   │   ├── app.py
│   │   │   └── requirement.txt
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── engine.py
│   │   ├── health.py
│   │   ├── metrics.py
│   │   ├── middleware.py
│   │   ├── openai_client.py
│   │   ├── phase6.py
│   │   ├── pipeline.py
│   │   ├── requirements.txt
│   │   ├── settings.py
│   │   └── utils.py
│   └── ui
│       ├── app.py
│       ├── Dockerfile.ui
│       ├── main.py
│       └── requirements.txt
├── connectors
│   └── requirements.txt
├── db
│   ├── 006_phase6_cache.sql
│   ├── init.sql
│   └── postgres.py
├── grafana
│   ├── dashboards
│   │   └── dashboards
│   └── datasource
├── infra
│   ├── alertmanager
│   │   └── alertmanager.yml
│   ├── connectors
│   │   ├── __init__.py
│   │   ├── Dockerfile
│   │   └── wikimedia_sse_connector.py
│   ├── grafana
│   │   └── provisioning
│   │       ├── dashboards
│   │       │   ├── dashboards.yml
│   │       │   ├── rag_indexer_dashboard.json
│   │       │   └── rag_indexer_overview.json
│   │       └── datasource
│   │           └── datasource.yml
│   ├── monitoring
│   │   ├── grafana
│   │   │   ├── dashboards
│   │   │   └── provisioning
│   │   └── prometheus
│   │       ├── alerts.yml
│   │       └── prometheus.yml
│   ├── postgres
│   │   ├── postgres
│   │   │   ├── 00_init.sql
│   │   │   └── init.sql
│   │   ├── __init__.py
│   │   ├── 001_init.sql
│   │   ├── 02_pgvector.sql
│   │   └── 03_rag_schema.sql
│   ├── prometheus
│   │   ├── rules
│   │   │   └── rag_indexer_alerts.yml
│   │   ├── alerts.yml
│   │   └── prometheus.yml
│   ├── redpanda.yaml
│   ├── .env
│   ├── alerts.yml
│   ├── docker-compose.yml
│   └── prometheus.yml
├── migrations
│   ├── 001_rag_schema.sql
│   ├── 002_indexes.sql
│   └── 003_retention.sql
├── monitoring
│   ├── grafana
│   │   ├── dashboards
│   │   │   └── wikimedia-rag-flow.json
│   │   └── provisioning
│   │       ├── dashboards
│   │       │   └── dashboards.yml
│   │       └── datasources
│   │           └── prometheus.yml
│   └── prometheus
│       └── prometheus.yml
├── observability
│   ├── grafana
│   │   ├── dashboards
│   │   │   └── wikimedia_rag_dashboard.json
│   │   └── provisioning
│   │       ├── dashboards
│   │       │   ├── dashboards.yml
│   │       │   ├── provider.yml
│   │       │   ├── wikimedia_rag_dashboard.json
│   │       │   └── wikimedia-rag-flow.json
│   │       └── datasources
│   │           └── datasource.yml
│   └── prometheus
│       └── prometheus.yml
├── postgres
│   └── init
│       └── 00_pgvector.sql
├── postgres-init
│   ├── 01-pgvector.sql
│   └── 01-vector.sql
├── prometheus
│   └── alerts.yml
├── rag
│   ├── api
│   │   └── requirements.txt
│   ├── common
│   └── phase6.py
├── scripts
│   ├── backfill_embeddings.py
│   └── wikimedia_sse_producer.py
├── spark
│   ├── spark-app
│   │   ├── spark_job.log
│   │   ├── spark_submit.log
│   │   └── spark_wikimedia_job.py
│   ├── spark-image
│   │   └── Dockerfile
│   ├── streaming
│   │   └── Dockerfile
│   └── Dockerfile
├── tools
│   ├── api_consistency_test.py
│   ├── api_proof.log
│   ├── api_proof.ps1
│   ├── db_migrate.ps1
│   ├── db_proof.log
│   ├── db_proof.ps1
│   ├── resiliency_demo.ps1
│   └── smoke_test.ps1
├── .env
├── .gitignore
├── __init__.py
├── consumer.in_image.py
├── docker-compose.grafana.yml
├── docker-compose.metrics.yml
├── docker-compose.metrics-host.yml
├── docker-compose.observability.yml
├── Dockerfile
├── Dockerfile.api
├── Dockerfile.indexer
├── Dockerfile.producer
├── Dockerfile.ui
├── gunicorn_conf.py
├── msg.json
├── producer_wikimedia.py
├── q.json
├── rag_api.py
├── README.md
└── requirements.txt
```
## Environment variables
  - Copy the sample file and fill values:
   ```text  cp .env.env ```
    ```text
    WIKIMEDIA_SSE_URL=https://stream.wikimedia.org/v2/stream/recentchange
    WIKI_FILTER=enwiki
    MAX_EVENTS_PER_MIN=60
    KAFKA_BROKERS=wikimedia-redpanda:9092
    KAFKA_TOPIC=wikimedia.cleaned
    PRODUCER_TOPIC=wikimedia.cleaned
    KAFKA_GROUP=rag-indexer-v3
    POSTGRES_DSN=postgresql://postgres:postgres@wikimedia-postgres:5432/postgres
    DATABASE_URL=postgresql://postgres:postgres@wikimedia-postgres:5432/postgres
    CHUNKS_TABLE=document_chunks
    EMBED_ENABLED=true
    EMBED_DIMS=1536
    OPENAI_EMBED_MODEL=text-embedding-3-small
    ANSWER_ENABLED=true
    OPENAI_CHAT_MODEL=gpt-4o-mini
    OPENAI_API_KEY=
    WIKIMEDIA_USER_AGENT="wikimedia-streaming-rag-bot/1.0 (https://github.com/.........; contact:.......)"
    ALLOWED_NAMESPACES=0
    FILTER_BOTS=true
    MAX_CONTENT_CHARS=700
    RECONNECT_SLEEP_SEC=2.0
    REQUEST_TIMEOUT_SEC=60
    KAFKA_FLUSH_EVERY=200
    LOG_LEVEL=INFO
    ```
## Quickstart
- **Start infrastructure**
      ```text docker compose -f .\infra\docker-compose.yml  ```
- **Initialize pgvector + schema**
      ```text  psql "host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER password=$PGPASSWORD" \-f sql/pgvector.sql ```
- **Install Python deps**
   ```text
   python -m venv .venv
   source .venv/bin/activate
   Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
- **Run ingestion (SSE → broker)**
- **Run stream processing**
- **Run indexing (gold → embeddings → pgvector)**
- **Run API (FastAPI)**
- **Open observability**
  - **Grafana:** ```text http://localhost:3000 ```
  - **Prometheus:** ```text http://localhost:9090 ```

## Conclusion
This project operationalizes Real-time RAG as a streaming data product, not a brittle chatbot demo, by ingesting Wikimedia’s SSE firehose into a broker-backed backbone (Redpanda/Kafka), continuously cleaning and shaping events with Spark Structured Streaming, governing the lifecycle through Medallion (Bronze/Silver/Gold) modeling, and serving retrieval through pgvector + FastAPI—resulting in a platform that’s durable under load (no data loss), replayable via offsets when logic changes, queryable through semantic + metadata search, explainable through citations and source context, and observable end-to-end with Prometheus + Grafana tracking lag, latency, and error rates—so instead of running on hope, you’re operating against measurable SLAs.

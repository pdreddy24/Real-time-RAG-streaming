# Real-time-RAG-streaming

Knowledge is a dynamic target. Data updates every minute, and if your system is not up to the mark, it becomes outdated already.

In this blog post, I will guide you through how I constructed the Streaming RAG platform on top of the WMF edit event streams. We aren’t merely constructing a chatbot. Rather, we’re constructing a data pipeline that’s durable, replayable, and observable.

**The Data Overview:**

To develop a real-time system, a high velocity data source is required. I pick the “Recent Changes” data stream provided by Wikimedia.

**Source:** Server-Sent Events (SSE) from Wikimedia

**Nature:** Semi-structured, messy, and continuous

**The Challenge:** It’s a firehose of inconsistent data. Our job is to turn this “noise” into “knowledge.”

**How I Built It in Phases:**
**The Architecture:**

_**Ingestion → Messaging → Stream Processing → Medallion Modeling → RAG Index → Retrieval API → UI → Observability.**_

This flow makes the system: Durable (broker-backed), replayable (offset-based), queryable (vector + metadata), observable (metrics-first), and demo-ready (UI + API).

**Phase 1: Ingestion Layer**

**The Concept:** Real-Time Data Capture Ingestion.The door to your system is Data Capture Ingestion. A world with streaming is a world in which data is not pushed every day as a series of batches, but instead as a constant stream. The idea here is that an event is captured by your system as soon as it occurs.

**Why SSE (Server-Sent Events)?:** The Wikimedia site has an SSE feed for every edit that happens. We use it because it’s a lightweight, one-way connection from server to us. Traditional APIs would require you to have to “ask” for notifications, but SSE keeps an open connection so that latency is minimal. This is how you would get a firehose feed.

**Phase 2: Messaging Layer**
The Concept: Buffer and the Backbone When data is flowing at a rapid pace, your processing engine may become overwhelmed and shut down. To provide a safety buffer, what is needed is a Message Broker. This concept brings about Durability (data is not lost) and Decoupling (the producer and the processor are not concerned with each other’s speed).

**Why Redpanda (or Kafka)?** Redpanda is a relatively new messaging platform built with C++ as a language. It is fully Kafka compatible. The main reason we use it is because it is very fast (low latency) compared to other systems (like Kafka) and is easier for us to administer (no JVM/ZooKeeper requirements). This enables us to “replay” information — if we discover a flaw in our AI logic, all we have to do is replay Redpanda.

**Phase 3: Stream Processing Layer**

**The Concept:** Continuous Transformation The data retrieved from the internet is unrefined. Stream Processing is the process of cleaning, filtering, and normalizing the data as it flows. This is how one avoids turning the “data lake” into a “data swamp.”

**Why Spark Structured Streaming?:** The industry standard for big data is Spark. We use its “Structured Streaming” engine because it takes a stream of information in exactly the same way that it deals with a fixed table. This is very helpful in creating complex queries that are handled seamlessly in the background by the system.

**Phase 4: Data Modeling**
**The Concept:** Medallion Architecture In production, you have to know exactly what the source of the data is. We apply the concept of Medallion Architecture to structure our information in three different levels − Bronze (raw), Silver (cleaned and valid), and Gold (ready to AI or User).

**why Medallion? **This pattern is used for governance traceability. Okay, suppose your chatbot provided an incorrect response. Well, you could trace your Gold record back into your Silver step of cleaning and observe your Bronze phase of the raw edit in order to understand what went precisely awry in your ‘black box’.”

**Phase 5: The RAG & Vector Layer**

**The Concept:** Semantic Retrieval Traditional databases match the word itself. Vector Retrieval matches the meaning. We represent text as numbers “embeddings,” where if the user requests information under “Climate Change,” the engine retrieving the material might look for edits involving “Global Warming,” even if the wording does not match.

**Why pgvector (PostgreSQL) + GPT-4?** I choose pgvector because we want to index our AI data in a standard Postgres database. It allows us to use the strength of vector-search without having to invest in a new, dedicated, and costly database solution. We will use the “Brain,” which is GPT-4, to understand the edits brought by the algorithm and generate a human-readable response.

**Phase 6: API Layer**

**The Concept: **The Service Contract There’s no point in having an AI pipeline if other apps cannot interact with it. The API (Application Programming Interface) layer allows for a Clean Contract — a predictable way for the user or UI to ask a question and receive a structured answer.

**Why FastAPI? **FastAPI is amongst the fastest python frameworks available. We employ FastAPI because it is native to Asynchronous (async) calls. Also, because making an LLM call or searching a vector store may take some time, FastAPI helps the server run seamlessly with hundreds of simultaneous users and not be “blocked” or slowed down.

**Phase 7: UI Layer**
**The Concept: **“Business Usability Engineers love logs and terminals. Stakeholders love buttons and graphs.” UI “is all about Explainability. It’s not just the answer — it’s about demonstrating to the user why the AI provided its response by displaying the source links used in the answer from Wikipedia.”

**Why a Custom UI? T**he custom UI facilitates the demonstration of the “Real-Time” aspect of our project. We will be able to display the actual feed of changes together with the query box. This will give our stakeholders a sense of the freshness of our content.

**Phase 8: Observability**

**The Concept:** Operational Truth In a stream processing system, being “working” is not a status. You must be able to answer the following questions. Is it falling behind? How long before the edit is searchable? This is the concept of Observability.

**Why Prometheus and Grafana?** Prometheus scrapes metrics (error rates and lag) every few seconds. Then, Grafana plots the numbers into beautiful, up-to-date graphs. The reason we are using these tools and services: If you don’t monitor Consumer Lag (how long it takes for an edit to go from the real world to your AI), you’re just “running on hope.”

**Conclusion**
Construction of a RAG production-grade infrastructure implies the ability to go from “just a script” to have a Durable Lifestyle. Therefore, with the utilization of Redpanda for security, Spark for cleaning, Medallion Modeling for trust, and Grafana for visibility, we transform the messy internet firehose into a dependable and trustable AI assistant.

# 🚀 Real-Time Crypto Market Streaming & Analytics Platform

<div align="center">

![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=for-the-badge&logo=apache-flink&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

**End-to-End Distributed Data Engineering Project**

*Built by [Deepak Patel](https://github.com/deepakpatel)*

</div>

---

## 📌 Overview

This platform ingests **live BTCUSDT cryptocurrency trade events** from Binance WebSocket API, streams them through Apache Kafka, processes and enriches them using PyFlink, and stores analytical results in ClickHouse — all running inside Docker Compose containers locally.

> A production-grade real-time streaming pipeline demonstrating the exact architecture used in **trading systems**, **fraud detection platforms**, and **real-time analytics** at scale.

---

## 🏗️ Architecture

```
Binance WebSocket API
        │
        ▼
Python WebSocket Producer
        │
        ▼
Kafka Topic: raw_market_data
        │
        ▼
┌─────────────────────────────────┐
│       PyFlink Stream Job        │
│  ├── JSON Parsing               │
│  ├── Data Enrichment            │
│  │     trade_value = p × q      │
│  │     is_anomaly = qty > 5     │
│  ├── Watermark Strategy (5s)    │
│  ├── Tumbling Windows (10s)     │
│  └── Aggregation                │
│        avg_price, trade_count   │
└─────────────────────────────────┘
        │
        ▼
Kafka Topic: processed_market_data
        │
        ▼
ClickHouse Kafka Engine Table
        │
        ▼
Materialized View
        │
        ▼
MergeTree Persistent Table
        │
        ▼
Analytics / BI / Dashboards
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Source** | Binance WebSocket API | Live BTCUSDT trade events |
| **Streaming** | Apache Kafka + Zookeeper | Distributed event streaming backbone |
| **Processing** | Apache Flink / PyFlink | Real-time stream processing engine |
| **Storage** | ClickHouse | Columnar OLAP analytics database |
| **Infrastructure** | Docker + Docker Compose | Containerized services |
| **Language** | Python | Producer logic & Flink job code |

---

## 🐳 Docker Services

| Service | Port | Role |
|---------|------|------|
| `zookeeper` | 2181 | Kafka cluster coordinator |
| `kafka` | 9092 | Distributed message broker |
| `kafka-ui` | 8080 | Web UI for topic monitoring |
| `flink-jobmanager` | 8081 | Flink job scheduling & coordination |
| `flink-taskmanager` | — | Flink worker nodes (actual processing) |
| `clickhouse` | 8123 / 9000 | OLAP columnar analytics database |

---

## ⚙️ PyFlink Processing Pipeline

```
Read Kafka  →  Parse JSON  →  Enrich  →  Watermark  →  Window  →  Aggregate  →  Sink Kafka
```

### Enrichment Fields

| Field | Formula | Purpose |
|-------|---------|---------|
| `trade_value` | `price × quantity` | USD value of each trade |
| `is_anomaly` | `quantity > 5` | Flag large / whale trades |
| `avg_price` | `AVG(price)` per window | 10-second market price snapshot |
| `trade_count` | `COUNT(*)` per window | Market activity intensity |

### Windowing

```
Timeline ──────────────────────────────────────────────────────▶
         │  Window 1  │  Window 2  │  Window 3  │  Window 4  │
         │   0 – 10s  │  10 – 20s  │  20 – 30s  │  30 – 40s  │
         └────────────┴────────────┴────────────┴────────────┘
              Each window: avg_price + trade_count computed independently
```

**Watermark:** 5-second delay tolerance for late/out-of-order events.

---

## 🗄️ ClickHouse Tables

| Table | Engine | Purpose |
|-------|--------|---------|
| `raw_trades` | Kafka Engine | Direct auto-ingestion from Kafka |
| `processed_trades` | MergeTree | Stores enriched trade events |
| `aggregated_metrics` | MergeTree | Windowed aggregates for BI/dashboards |

---

## 🚦 Getting Started

### Prerequisites

- Docker & Docker Compose installed
- Python 3.8+

### 1. Clone the Repository

```bash
git clone https://github.com/deepakpatel/crypto-streaming-platform.git
cd crypto-streaming-platform
```

### 2. Start All Services

```bash
docker-compose up -d
```

### 3. Verify All Containers Are Running

```bash
docker ps
```

### 4. Start the Binance WebSocket Producer

```bash
docker exec -it producer python producer/main.py
```

### 5. Submit the PyFlink Streaming Job

```bash
docker exec -it flink-jobmanager \
  flink run -py /opt/flink/jobs/flink_job.py
```

### 6. Open Dashboards

```bash
# Kafka UI — monitor topics and messages
open http://localhost:8080

# Flink Web Dashboard — monitor jobs
open http://localhost:8081
```

---

## 📡 Kafka Commands

```bash
# List all topics
docker exec -it kafka kafka-topics.sh \
  --list --bootstrap-server localhost:9092

# Watch live raw trade events
docker exec -it kafka kafka-console-consumer.sh \
  --topic raw_market_data \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Watch live processed events
docker exec -it kafka kafka-console-consumer.sh \
  --topic processed_market_data \
  --bootstrap-server localhost:9092
```

---

## 🔍 ClickHouse Queries

```bash
# Open ClickHouse client
docker exec -it clickhouse clickhouse-client

# View latest aggregated metrics
SELECT * FROM aggregated_metrics
ORDER BY window_start DESC
LIMIT 20;

# Average BTC price per 10-second window
SELECT
    window_start,
    avg_price,
    trade_count
FROM aggregated_metrics
ORDER BY window_start DESC;

# View anomalous trades
SELECT * FROM processed_trades
WHERE is_anomaly = true
ORDER BY event_time DESC
LIMIT 50;
```

---

## 🔄 Stop the Platform

```bash
# Stop all services
docker-compose down

# Stop and delete all data volumes
docker-compose down -v
```

---

## 💡 Key Engineering Concepts

### 🔄 Event-Driven Architecture
Entire system is async. Kafka decouples all producers and consumers — no service polls another. Events flow continuously and independently.

### 🪟 Tumbling Event-Time Windows
Flink groups events into non-overlapping 10-second buckets. Each window independently computes `avg_price` and `trade_count` — the core pattern in financial stream analytics.

### 💧 Watermarking
Handles late/out-of-order events. Tells Flink: *"wait 5 seconds before closing a window."* Without watermarks, aggregations are incorrect for delayed messages.

### 📦 Raw Data Preservation
`raw_market_data` topic is **never discarded**. This enables:
- Debugging incorrect outputs
- Financial audit trails
- Replaying with updated business logic
- Full data recovery on downstream failure

### ⚡ OLAP vs OLTP
ClickHouse is columnar OLAP — data stored column-by-column. Makes `AVG`, `COUNT`, `GROUP BY` aggregations **10–100x faster** than row-based databases like MySQL or PostgreSQL.

### 🔁 Stateful Stream Processing
Flink maintains state across multiple events within a window — enabling running averages and counts, not just per-event processing.

---

## 🐛 Challenges & Solutions

| Challenge | Problem | Solution |
|-----------|---------|----------|
| PyFlink Serialization | Row type caused serialization errors | Defined explicit `TypeInformation` for all output fields |
| Kafka Sink Output | Sink couldn't serialize Python objects | Used `SimpleStringSchema` with JSON string conversion |
| Docker Networking | Containers couldn't reach each other | Configured bridge network; used service names as hostnames |
| Python Imports | Module imports failed in Flink containers | Set `PYTHONPATH` correctly in `docker-compose.yml` |
| Data Persistence | ClickHouse data lost on `docker-compose down` | Added named volumes in `docker-compose.yml` |
| Window Accuracy | Windows closed before late events arrived | Used `BoundedOutOfOrdernessWatermarks` with 5s tolerance |

---

## 🗺️ Future Roadmap

- [ ] **Schema Registry** — Enforce Avro/Protobuf schemas at Kafka topic level
- [ ] **Exactly-Once Processing** — Flink + Kafka transactions, zero duplicates
- [ ] **Kubernetes Deployment** — Auto-scale Flink workers and Kafka brokers
- [ ] **Grafana + Prometheus** — Real-time dashboards and alerting
- [ ] **ML Anomaly Detection** — Replace rule-based with Isolation Forest model
- [ ] **Flink Checkpointing** — Periodic state snapshots for fault tolerance
- [ ] **Multi-Symbol Streaming** — ETHUSDT, SOLUSDT, BNBUSDT simultaneously
- [ ] **dbt Transformations** — Data modeling layer on top of ClickHouse
- [ ] **Apache Iceberg** — Lakehouse storage with time-travel queries
- [ ] **Multi-Broker Kafka** — 3+ brokers, replication factor = 3
- [ ] **Airflow Orchestration** — Schedule and monitor pipeline health
- [ ] **CI/CD Pipeline** — GitHub Actions for automated testing & deployment

---

## 📁 Project Structure

```
crypto-streaming-platform/
│
├── docker-compose.yml          # All 6 services defined here
│
├── producer/
│   └── main.py                 # Binance WebSocket → Kafka producer
│
├── flink_jobs/
│   └── flink_job.py            # PyFlink streaming job
│
├── clickhouse/
│   └── init.sql                # ClickHouse table definitions
│
└── README.md
```

---

## 👤 Author

**Deepak Patel**

> *"Raw data should never be lost. Processed data should be reproducible. Streaming systems must handle late events."*

---

<div align="center">

⭐ **Star this repo if you found it helpful!**

</div>

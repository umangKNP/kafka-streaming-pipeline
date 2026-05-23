# kafka-streaming-pipeline

A real-time stock price aggregation pipeline using Kafka, Python, and PostgreSQL.
Publishes synthetic stock tick events, computes VWAP (volume-weighted average price)
per symbol per 1-minute window, and stores results with a live CLI dashboard.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.6-black?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│   ┌─────────────┐         ┌─────────────────────────────────┐  │
│   │   Kafka     │         │          PostgreSQL              │  │
│   │  :9092      │         │  ┌─────────────────────────┐    │  │
│   │             │         │  │  price_aggregates        │    │  │
│   │ stock-ticks │         │  │  (symbol, window, vwap)  │    │  │
│   │ stock-ticks │         │  └─────────────────────────┘    │  │
│   │    -dlq     │         │  ┌─────────────────────────┐    │  │
│   └─────────────┘         │  │  dead_letter_queue       │    │  │
│          ▲  │             │  └─────────────────────────┘    │  │
└──────────|──|─────────────└─────────────────────────────────┘  │
           │  │                              ▲                    
           │  ▼                              │                    
    ┌──────────────┐              ┌──────────────────┐           
    │  producer.py │              │   consumer.py    │           
    │              │              │                  │           
    │ Generates    │ stock-ticks  │ - Reads ticks    │           
    │ fake ticks   │─────────────▶│ - 1-min VWAP     │           
    │ (5 symbols,  │              │ - Upserts to PG  │           
    │  500ms each) │              │ - DLQ on 3 fails │           
    └──────────────┘              └──────────────────┘           
                                           │                     
                                           ▼                     
                                  ┌──────────────────┐           
                                  │  dashboard.py    │           
                                  │                  │           
                                  │ Live rich table  │           
                                  │ refreshes every  │           
                                  │ 5 seconds        │           
                                  └──────────────────┘           
```

## Key Design Decisions

### Why VWAP?
VWAP (Volume-Weighted Average Price) is the standard institutional benchmark for
execution quality. Unlike a simple mean, VWAP weights prices by trade size —
a 10,000-share trade at $185 matters more than a 100-share trade at $190.

Formula: `VWAP = Σ(price × volume) / Σ(volume)`

### Why 1-minute windows?
1-minute is the standard granularity for intraday analytics. Short enough to detect
momentum shifts, long enough to smooth microstructure noise.

### Why upsert instead of insert?
Consumer rebalancing or restarts can re-process messages. Upsert ensures idempotency —
replaying the same window produces the same result rather than duplicate rows.

### Dead Letter Queue
Messages that fail 3 consecutive processing attempts are routed to `stock-ticks-dlq`
and written to the `dead_letter_queue` table for manual inspection and replay.
This prevents a single malformed message from blocking the consumer indefinitely.

---

## Quickstart

**Prerequisites:** Docker, Docker Compose, Python 3.11+

```bash
# 1. Start Kafka and Postgres
docker compose up -d

# 2. Wait ~15 seconds for services to be healthy, then install deps
pip install -r requirements.txt

# 3. Terminal 1 — start the producer
python producer.py

# 4. Terminal 2 — start the consumer
python consumer.py

# 5. Terminal 3 — open the live dashboard
python dashboard.py
```

## Run Tests

```bash
pip install pytest
pytest tests/ -v
```

## Project Structure

```
kafka-streaming-pipeline/
├── docker-compose.yml      # Kafka + Postgres services
├── sql/
│   └── init.sql            # Table definitions
├── producer.py             # Tick generator → Kafka
├── consumer.py             # Kafka → VWAP aggregation → Postgres
├── dashboard.py            # Live rich CLI dashboard
├── requirements.txt
└── tests/
    └── test_consumer.py    # Unit tests for VWAP logic
```

## Potential Extensions

- **Schema Registry + Avro** — enforce message schema at the broker level
- **Flink / Spark Streaming** — replace the Python consumer for true distributed processing
- **Grafana dashboard** — connect to Postgres for visual time-series charts
- **Multiple partitions** — partition by symbol for parallel consumer scaling
- **Exactly-once semantics** — Kafka transactions to remove the upsert requirement

# Real-Time Election Analytics


## 🌟 Project Overview
![Architecture Diagram](https://raw.githubusercontent.com/MOdiaa2003/Real_time-_election/e1f377aaed177694565b8479e22ea51d2546fb5f/images/arch.drawio%20(1).png)

A simulation of an election system that generates voter and candidate data, processes votes in real time, and visualizes key metrics. The pipeline leverages:

- **Python scripts** for data generation and Kafka integration
- **PostgreSQL** for persistent storage
- **Apache Kafka** for streaming message transport
- **Apache Spark Structured Streaming** for real-time aggregations
- **Streamlit** dashboard for live visualization
- **Docker Compose** for local environment setup

---

## 🏗️ Architecture

```text
┌────────────┐       ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│ Generator  │───▶   │  Kafka      │───▶   │  Spark      │───▶   │  Kafka    │
│ Scripts    │       │  (topics)   │       │ Streaming   │       │ (agg-topics)│
└────────────┘       └─────────────┘       └────┬────────┘       └────┬────────┘
                                               │                     │
                                               ▼                     ▼
                                         ┌────────────┐        ┌─────────────┐
                                         │PostgreSQL  │        │Streamlit    │
                                         │(votes data)│        │Dashboard    │
                                         └────────────┘        └─────────────┘
```

---

## 🧰 Prerequisites

- Docker & Docker Compose
- Python 3.9+ with dependencies installed (`pip install -r requirements.txt`)
- Network connectivity to pull Docker images

---

## 🚀 Setup & Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/MOdiaa2003/Real_time-_election.git
   cd Real_time-_election
   ```

2. **Start services**
   ```bash
   docker-compose up -d
   ```
   This brings up Zookeeper, Kafka broker, PostgreSQL, and Spark Master/Worker.

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run data generators**
   ```bash
   python main.py   # Generates and streams voter/candidate data to Kafka and PostgreSQL
   ```

5. **Run vote processor**
   ```bash
   python voting.py     # Consumes voters, assigns votes, writes to DB and Kafka
   ```

6. **Start Spark streaming**
   ```bash
   spark-submit spark_streaming.py  # Reads votes_topic, writes aggregations back to Kafka
   ```

7. **Launch Dashboard**
   ```bash
   streamlit run streamlit-app.py   # Visualizes metrics in real time
   ```

---

## 📦 Components

### 1. `main.py`
**Non-Technical:** Generates fake voter and candidate profiles, stores them in PostgreSQL, and streams them into Kafka topics (`voters_topic`, `candidates_topic`).

**Technical:** Uses RandomUser API to fetch 25,000 UK users per batch, assigns `education_level` and `party`, creates PostgreSQL tables (`voters`, `candidates`), inserts records via `psycopg2`, and produces messages with Confluent Kafka `SerializingProducer`.

---

### 2. `voting.py`
**Non-Technical:** Listens for new voter profiles, randomly assigns each a candidate, polling station, voting method, timestamp, and income category, then records and streams the vote.

**Technical:** Configures Kafka `Consumer` to read `voters_topic`, fetches candidates from PostgreSQL, uses `dict` union to merge data, writes each vote to `votes` table, and publishes enriched vote JSON to `votes_topic` using `SerializingProducer`.

---

### 3. `spark_streaming.py`
**Non-Technical:** Continuously aggregates incoming votes to compute metrics like votes per candidate, turnout by region, top polling stations, education-party breakdown, and hourly turnout.

**Technical:** Spark Structured Streaming job under Spark 3.5.3; reads JSON from `votes_topic`, defines schema, applies watermark, computes six aggregations (`votes_per_candidate`, `turnout_by_location`, `top_polling_stations`, `voters_by_education_party`, `turnout_by_hour`), and writes results back to Kafka topics with checkpointing for exactly-once semantics.

---

### 4. `streamlit-app.py`
**Non-Technical:** Real‑time dashboard displaying total voters/candidates, leading candidate, top regions, polling stations, education-party charts, and hourly vote trends.

**Technical:** Streamlit app using `kafka-python` to consume aggregated topics, `psycopg2` for summary stats, `plotly.express` and `matplotlib` for interactive charts, plus `st_autorefresh` for automatic updates.

---

## 🐳 `docker-compose.yml`

Defines services:
- **Zookeeper** (Confluent CP 7.4.0)
- **Kafka Broker** (Confluent CP 7.4.0)
- **PostgreSQL** (voting DB)
- **Spark Master & Worker** (Bitnami Spark)

Use `docker-compose ps` to verify all containers are healthy.

---

## 📄 License

This project is licensed under the MIT License. See `LICENSE` for details.


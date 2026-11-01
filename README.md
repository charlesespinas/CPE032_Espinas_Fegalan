# 🎌 Anime Streaming Dashboard

**Pipeline:** Jikan API → Kafka Producer → Kafka → Spark Structured Streaming → MongoDB → Streamlit

| View | Flow |
| --- | --- |
| Real-time | Kafka → Streamlit (live consumer, auto-refresh 5–60 s) |
| Historical | Kafka → Spark → MongoDB → Streamlit (filters, aggregation, export) |

## Data
Source: https://docs.api.jikan.moe/ (`/top/anime`, `/seasons/now`). Each API item becomes one message per metric
(`score, scored_by, rank, popularity, members, favorites`) using the required schema
`timestamp, value, metric_type, sensor_id` plus `title, genres, anime_type, status, year, episodes`.

## Setup
```bash
conda create -n bigdata python=3.10.13
conda activate bigdata
pip install -r requirements.txt
cp .env.example .env
docker compose up -d          # Kafka + MongoDB (or use your own installs)
```
Spark 3.5 needs Java 8/11/17 (check with `java -version`). No separate Spark install is needed: `pyspark==3.5.1` comes from requirements.txt.

## Run (4 terminals)
```bash
python producer.py --interval 10 --source top
python spark_consumer.py   # uses pip pyspark 3.5.1 + Kafka connector (downloaded on first run)
streamlit run app.py
```

## Features
- Live dashboard (top-N bar, score vs members scatter, per-anime trend)
- Full history in MongoDB with time-range / metric / anime filters
- Aggregation: Spark (per-batch metric stats, genre avg score) + hourly/daily/weekly in dashboard
- Export CSV, Excel (.xlsx), PDF
- Error handling: API retries/429 back-off, Kafka/Mongo connection errors shown in UI

## Files
`app.py`, `producer.py`, `spark_consumer.py`, `requirements.txt`, `README.md`, `.env.example`, `docker-compose.yml`
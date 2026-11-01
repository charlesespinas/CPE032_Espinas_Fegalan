"""
Kafka Producer - Jikan API (MyAnimeList) -> Kafka

Fetches anime statistics from https://api.jikan.moe/v4 and publishes ONE
message per (anime, metric) so the dashboard schema stays:
    timestamp, value, metric_type, sensor_id
plus extra descriptive fields (title, genres, type, year, status ...).

Jikan rate limits: 3 requests/sec, 60 requests/min -> we make 1 request per cycle.
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv()

DEFAULT_BASES = os.getenv("JIKAN_BASE_URLS", "https://api.jikan.moe/v4").split(",")
METRICS = ["score", "scored_by", "rank", "popularity", "members", "favorites"]


class JikanClient:
    """Wrapper around the Jikan v4 REST API with retry, rate-limit handling
    and optional fallback base URLs (e.g. a Jikan-compatible mirror)."""

    def __init__(self, bases: List[str], max_retries: int = 3):
        self.bases = [b.strip().rstrip("/") for b in bases if b.strip()]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "bigdata-streaming-dashboard/1.0"})
        self.max_retries = max_retries

    def get(self, path: str, params: Optional[dict] = None) -> Optional[dict]:
        for attempt in range(1, self.max_retries + 1):
            for base in self.bases:
                try:
                    resp = self.session.get(f"{base}{path}", params=params, timeout=(8, 15))
                    if resp.status_code == 429:  # rate limited
                        print(f"[API] {base} returned 429 (rate limited)")
                        time.sleep(2 ** attempt)
                        continue
                    resp.raise_for_status()
                    return resp.json()
                except requests.RequestException as e:
                    print(f"[API] {base} failed: {type(e).__name__}")
            wait = 2 ** attempt
            print(f"[API] attempt {attempt}/{self.max_retries} failed on all hosts, retry in {wait}s")
            time.sleep(wait)
        return None

    def top_anime(self, page: int = 1, limit: int = 25) -> List[dict]:
        data = self.get("/top/anime", {"page": page, "limit": limit})
        return data.get("data", []) if data else []

    def current_season(self, page: int = 1, limit: int = 25) -> List[dict]:
        data = self.get("/seasons/now", {"page": page, "limit": limit})
        return data.get("data", []) if data else []


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str, source: str = "top", bases=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.source = source
        self.api = JikanClient(bases or DEFAULT_BASES)
        self.page = 1
        self.max_pages = 4  # cycle pages 1..4 (top 100 anime)
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=20,
            )
            print(f"Kafka producer connected to {self.bootstrap_servers}, topic '{self.topic}'")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {self.bootstrap_servers}")
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")

    # ------------------------------------------------------------------ data
    def fetch_records(self) -> List[Dict[str, Any]]:
        """Call Jikan once and convert the response to flat metric records."""
        items = (
            self.api.top_anime(self.page)
            if self.source == "top"
            else self.api.current_season(self.page)
        )
        print(f"[API] source={self.source} page={self.page} -> {len(items)} anime")
        self.page = self.page % self.max_pages + 1

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records = []
        for a in items:
            genres = ", ".join(g["name"] for g in a.get("genres", []))
            common = {
                "timestamp": now,
                "sensor_id": str(a["mal_id"]),
                "title": a.get("title_english") or a.get("title"),
                "genres": genres,
                "anime_type": a.get("type"),
                "status": a.get("status"),
                "year": a.get("year"),
                "episodes": a.get("episodes"),
                "source": f"jikan:{self.source}",
            }
            for metric in METRICS:
                val = a.get(metric)
                if val is None:
                    continue
                try:
                    records.append({**common, "metric_type": metric, "value": float(val)})
                except (TypeError, ValueError):
                    continue
        return records

    # ----------------------------------------------------------------- kafka
    def send_batch(self, records: List[Dict[str, Any]]) -> int:
        if not self.producer:
            print("ERROR: Kafka producer not initialized, trying to reconnect...")
            self._connect()
            return 0
        sent = 0
        try:
            for r in records:
                self.producer.send(self.topic, key=r["sensor_id"], value=r)
                sent += 1
            self.producer.flush(timeout=15)
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return 0
        return sent

    def produce_stream(self, interval: float = 10.0, duration: Optional[int] = None):
        print(f"Starting producer: 1 API call every {interval}s, duration: {duration or 'infinite'}")
        start, total = time.time(), 0
        try:
            while True:
                if duration and time.time() - start >= duration:
                    print(f"Reached duration limit of {duration}s")
                    break
                records = self.fetch_records()
                if records:
                    n = self.send_batch(records)
                    total += n
                    print(f"[{datetime.now():%H:%M:%S}] sent {n} messages (total {total})")
                else:
                    print("No records fetched this cycle.")
                time.sleep(max(interval, 1.0))
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {total}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed")
            except Exception as e:
                print(f"Error closing producer: {e}")


def parse_arguments():
    p = argparse.ArgumentParser(description="Jikan API -> Kafka producer")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "streaming-data"))
    p.add_argument("--interval", type=float, default=10.0, help="Seconds between API calls (>=1)")
    p.add_argument("--duration", type=int, default=None, help="Run time in seconds (default infinite)")
    p.add_argument("--source", choices=["top", "season"], default="top",
                   help="top = top anime ranking, season = currently airing anime")
    p.add_argument("--api-base", action="append", default=None,
                   help="Jikan-compatible base URL (repeatable, tried in order). "
                        "Default: https://api.jikan.moe/v4")
    return p.parse_args()


def main():
    args = parse_arguments()
    producer = StreamingDataProducer(args.bootstrap_servers, args.topic, args.source, args.api_base)
    producer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()
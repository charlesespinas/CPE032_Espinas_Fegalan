"""
Spark Structured Streaming: Kafka -> (parse + aggregate) -> MongoDB

Run (just this, the Kafka connector is loaded automatically):
    python spark_consumer.py

Writes:
    <db>.anime_raw  : every message (history)
    <db>.anime_agg  : per-micro-batch aggregates (by metric_type and by genre)
"""

import os
from datetime import datetime


os.environ.pop("SPARK_HOME", None)
for _var in ("HADOOP_CONF_DIR", "YARN_CONF_DIR"):
    os.environ.pop(_var, None)

from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "anime_db")
RAW_COLLECTION = os.getenv("MONGO_RAW_COLLECTION", "anime_raw")
AGG_COLLECTION = os.getenv("MONGO_AGG_COLLECTION", "anime_agg")
CHECKPOINT_DIR = "file://" + os.path.abspath("checkpoints/anime")

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("title", StringType()),
    StructField("genres", StringType()),
    StructField("anime_type", StringType()),
    StructField("status", StringType()),
    StructField("year", StringType()),
    StructField("episodes", StringType()),
    StructField("source", StringType()),
])


def parse_ts(s: str):
    """ISO string (UTC, ends with Z) -> naive UTC datetime (pymongo stores as UTC)."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def write_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        db = client[MONGO_DB]
        now = datetime.utcnow()

        raw_docs = []
        for row in batch_df.collect():
            d = row.asDict()
            try:
                d["timestamp"] = parse_ts(d["timestamp"])
            except (TypeError, ValueError):
                continue
            raw_docs.append(d)
        if raw_docs:
            db[RAW_COLLECTION].insert_many(raw_docs)

        metric_agg = (
            batch_df.groupBy("metric_type")
            .agg(
                F.count("*").alias("count"),
                F.round(F.avg("value"), 3).alias("avg"),
                F.min("value").alias("min"),
                F.max("value").alias("max"),
            )
            .collect()
        )
        agg_docs = [
            {"kind": "metric", "batch_id": batch_id, "batch_time": now, **r.asDict()}
            for r in metric_agg
        ]

        genre_agg = (
            batch_df.filter(F.col("metric_type") == "score")
            .withColumn("genre", F.explode(F.split("genres", ", ")))
            .filter(F.col("genre") != "")
            .groupBy("genre")
            .agg(F.count("*").alias("count"), F.round(F.avg("value"), 3).alias("avg_score"))
            .collect()
        )
        agg_docs += [
            {"kind": "genre", "batch_id": batch_id, "batch_time": now, **r.asDict()}
            for r in genre_agg
        ]
        if agg_docs:
            db[AGG_COLLECTION].insert_many(agg_docs)

        print(f"[batch {batch_id}] raw={len(raw_docs)} agg={len(agg_docs)} -> MongoDB")
    finally:
        client.close()


def main():
    spark = (
        SparkSession.builder.appName("AnimeKafkaToMongo")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        stream.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .filter(F.col("value").isNotNull() & F.col("sensor_id").isNotNull())
    )

    query = (
        parsed.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
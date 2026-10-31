"""
spark_streaming_job.py - Part D: Spark Structured Streaming.

Equivalent of the Python windowed processor:
    5-second event-time tumbling window  x  job_type  ->  count, avg_duration

Sources (choose with --source):
  file  : the extended stream is split into small CSV files in data/stream_input/;
          Spark reads ONE file per micro-batch (maxFilesPerTrigger=1), i.e. the
          files behave like arriving micro-batches. No Kafka needed.
  kafka : reads JSON events from topic `research-events` (run kafka_producer.py).

Run from the project root:
    python src/spark_streaming_job.py --source file
    python src/spark_streaming_job.py --source kafka
    python src/spark_streaming_job.py --source kafka --live 120   # keep running 120 s
(or: spark-submit src/spark_streaming_job.py --source file)
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (BOOTSTRAP, DATA_DIR, EVENT_DATE, FIELDS, JOB_TYPES, RESULTS_DIR,  # noqa: E402
                    TOPIC, generate_extended_stream)

STREAM_DIR = DATA_DIR / "stream_input"


def build_spark(with_kafka: bool):
    import pyspark
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .appName("AITU-A4-PartD-StructuredStreaming")
               .master("local[3]")                              # 3 local cores = 3 parallel tasks
               .config("spark.sql.shuffle.partitions", "3")     # small data -> few shuffle partitions
               .config("spark.sql.session.timeZone", "UTC"))    # show 14:00:00 exactly as in the data
    if with_kafka:
        v = pyspark.__version__
        scala = "2.13" if v.startswith("4") else "2.12"
        builder = builder.config("spark.jars.packages",
                                 f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{v}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def event_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType
    return StructType([
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("research_group", StringType()),
        StructField("job_type", StringType()),
        StructField("duration_sec", IntegerType()),
        StructField("status", StringType()),
        StructField("cpu_pct", IntegerType()),
        StructField("arrival_seq", IntegerType()),
    ])


def prepare_stream_files(events_per_file: int = 12) -> int:
    """Write the extended stream (arrival order) as small CSV files = micro-batches."""
    if STREAM_DIR.exists():
        shutil.rmtree(STREAM_DIR)
    STREAM_DIR.mkdir(parents=True)
    events = generate_extended_stream()
    cols = FIELDS + ["arrival_seq"]
    n_files = 0
    for i in range(0, len(events), events_per_file):
        with open(STREAM_DIR / f"batch_{n_files:03d}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(events[i:i + events_per_file])
        n_files += 1
    return n_files


def read_stream(spark, source: str, bootstrap: str = BOOTSTRAP):
    from pyspark.sql import functions as F
    if source == "file":
        return (spark.readStream
                .option("header", True)
                .option("maxFilesPerTrigger", 1)
                .schema(event_schema())
                .csv(str(STREAM_DIR)))
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())
    return (raw.select(F.from_json(F.col("value").cast("string"), event_schema()).alias("e"))
               .select("e.*"))


def windowed_job_type_stats(events_df):
    """THE Part D transformation: window, job_type, count, avg_duration."""
    from pyspark.sql import functions as F
    parsed = (events_df
              .withColumn("event_time",
                          F.to_timestamp(F.concat_ws(" ", F.lit(EVENT_DATE), F.col("timestamp")),
                                         "yyyy-MM-dd HH:mm:ss"))
              # schema validation inside Spark: drop malformed rows
              .filter(F.col("event_time").isNotNull()
                      & F.col("job_type").isin(*JOB_TYPES)
                      & (F.col("duration_sec") > 0)))
    return (parsed
            .withWatermark("event_time", "10 seconds")          # tolerate events up to 10 s late
            .groupBy(F.window("event_time", "5 seconds"), F.col("job_type"))
            .agg(F.count("*").alias("count"),
                 F.round(F.avg("duration_sec"), 2).alias("avg_duration")))


def save_result_csv(rows, path: Path):
    path.parent.mkdir(exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["window_start", "window_end", "job_type", "count", "avg_duration"])
        for r in rows:
            w.writerow([r["window"]["start"].strftime("%H:%M:%S"),
                        r["window"]["end"].strftime("%H:%M:%S"),
                        r["job_type"], r["count"], r["avg_duration"]])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["file", "kafka"], default="file")
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--live", type=int, default=0,
                    help="keep the query running N seconds (prints every micro-batch)")
    args = ap.parse_args()

    if args.source == "file":
        print(f"prepared {prepare_stream_files()} micro-batch files in {STREAM_DIR}")

    spark = build_spark(with_kafka=(args.source == "kafka"))
    events = read_stream(spark, args.source, args.bootstrap)
    result = windowed_job_type_stats(events)
    result.printSchema()

    ckpt = tempfile.mkdtemp(prefix="a4_ckpt_")
    console_q = (result.orderBy("window", "job_type").writeStream
                 .outputMode("complete")
                 .format("console")
                 .option("truncate", False)
                 .option("numRows", 200)
                 .option("checkpointLocation", ckpt + "/console")
                 .start())
    memory_q = (result.writeStream
                .outputMode("complete")
                .format("memory")
                .queryName("job_type_windows")
                .option("checkpointLocation", ckpt + "/memory")
                .start())

    if args.live:
        console_q.awaitTermination(args.live)
    else:
        console_q.processAllAvailable()
    memory_q.processAllAvailable()

    rows = spark.sql("SELECT * FROM job_type_windows ORDER BY window, job_type").collect()
    out = RESULTS_DIR / f"spark_partD_{args.source}.csv"
    save_result_csv(rows, out)
    print(f"\n{len(rows)} result rows saved to {out}")
    print("last micro-batch progress:", console_q.lastProgress)

    console_q.stop()
    memory_q.stop()
    spark.stop()


if __name__ == "__main__":
    main()

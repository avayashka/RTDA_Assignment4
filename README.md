# Assignment 4 — Distributed Stream Processing
AITU Research Event Analytics with Kafka and Spark (synthetic data)


## 1. Project structure

```
assignment4_kafka_spark/
├── Assignment4.ipynb          <- MAIN notebook: Parts B, C, D, E (run top to bottom)
├── README.md                  <- this file
├── requirements.txt
├── docker-compose.yml         <- Kafka (KRaft) + Kafka UI, only for the optional Kafka runs
├── src/
│   ├── common.py              <- schema, 10 supplied events, validator, extended-stream generator
│   ├── kafka_producer.py      <- Part B with a real broker (optional)
│   ├── kafka_consumer.py      <- Part B with a real broker (optional), consumer group + DLQ + merge
│   ├── spark_streaming_job.py <- Part D standalone (file source or Kafka source)
│   └── load_test.py           <- Part E (in-process or Kafka mode)
├── data/                      <- created by the notebook
│   ├── events_initial.csv     <- 10 supplied events
│   ├── events_extended.csv    <- supplied + generated events (Part C/D)
│   └── stream_input/          <- micro-batch CSV files read by Spark
├── results/                   <- every table and chart produced (evidence for the report)
└── docs/architecture.png      <- Part A diagram
```

## 2. Installation

### Python (all parts)
Use **Python 3.10 or 3.11** (PySpark 3.5 does not support 3.13).
```bash
cd assignment4_kafka_spark
python -m venv .venv
# Windows:  .venv\Scripts\activate        macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook          # open Assignment4.ipynb
```

### Java (needed by Spark)
Spark 3.5 runs on **Java 17** (8/11 also work).
* Windows/macOS: install *Eclipse Temurin 17* (adoptium.net), then check `java -version`.
* Ubuntu/WSL: `sudo apt install openjdk-17-jdk`
* If Spark cannot find Java, set `JAVA_HOME` to the JDK folder.

**Windows note:** Spark streaming writes checkpoints through Hadoop libraries, which on native Windows need
`winutils.exe` + `hadoop.dll` (`HADOOP_HOME`). The easy ways around it: run the project in **WSL2 (Ubuntu)**, or run
the notebook in **Google Colab** (Spark works there out of the box, see §5).

### Kafka (optional for B/E, needed for the Kafka source in D)
Easiest: **Docker Desktop** + the included compose file.
```bash
docker compose up -d                 # starts Kafka on localhost:9092 and Kafka UI on http://localhost:8080
docker ps                            # both containers should be "Up"
# create the topic explicitly (the producer also does it automatically):
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic research-events --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic research-events
```
Without Docker (Linux/WSL/Colab): download Kafka 3.8 binaries and start KRaft mode:
```bash
wget https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz && tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```

## 3. Run order

| Step | Command | Output / evidence |
|---|---|---|
| 1 | Run `Assignment4.ipynb` top to bottom | `results/partB_*`, `partC_*`, `partD_*`, `partE_*`, `summary.json`, charts |
| 2 (optional) | `docker compose up -d` | Kafka + UI running |
| 3 (optional) | terminal 1: `python src/kafka_consumer.py --name c1` | consumer waits |
| 4 (optional) | terminal 2: `python src/kafka_producer.py --bad` | partition/offset per event |
| 5 (optional) | consumer prints validation lines + tables, saves `results/consumer_c1.json` | Part B tables from Kafka |
| 6 (optional) | 3 consumers in parallel: `--name c1`, `c2`, `c3`, `--group research-analytics-3` then `python src/kafka_consumer.py --merge` | partitions split between consumers; merged result |
| 7 | `python src/spark_streaming_job.py --source file` | console micro-batches + `results/spark_partD_file.csv` |
| 8 (optional) | `python src/spark_streaming_job.py --source kafka` | Spark reads the Kafka topic |
| 9 (optional) | `python src/load_test.py --mode kafka` | Part E through the broker |

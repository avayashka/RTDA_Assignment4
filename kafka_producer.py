"""
kafka_producer.py - Part B (Kafka version): send research-job events to Kafka.

  * creates topic `research-events` with 3 partitions (if it does not exist)
  * key = research_group -> all events of one group land in the same partition
    (ordering per group is preserved, load is spread over 3 partitions)
  * value = JSON event
  * acks="all" + retries -> no silent loss if the broker is slow

Run from the project root (Kafka must be running, see README):
    python src/kafka_producer.py                      # 10 supplied events
    python src/kafka_producer.py --extended           # 10 supplied + generated events
    python src/kafka_producer.py --rate 5             # 5 events per second
    python src/kafka_producer.py --bad                # also send 2 invalid events (tests DLQ)
"""
import argparse
import json
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from common import (BOOTSTRAP, DLQ_TOPIC, INITIAL_EVENTS, NUM_PARTITIONS, TOPIC,
                    generate_extended_stream)


def ensure_topics(bootstrap: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="a4-admin")
    for name, parts in ((TOPIC, NUM_PARTITIONS), (DLQ_TOPIC, 1)):
        try:
            admin.create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)])
            print(f"[admin] created topic {name} with {parts} partition(s)")
        except TopicAlreadyExistsError:
            print(f"[admin] topic {name} already exists")
    admin.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--rate", type=float, default=2.0, help="events per second (0 = no delay)")
    ap.add_argument("--extended", action="store_true", help="send the extended synthetic stream")
    ap.add_argument("--bad", action="store_true", help="append two malformed events")
    args = ap.parse_args()

    ensure_topics(args.bootstrap)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        client_id="research-producer-1",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=5,
    )

    events = generate_extended_stream() if args.extended else [dict(e) for e in INITIAL_EVENTS]
    for e in events:
        e.pop("arrival_seq", None)
    if args.bad:
        events.append({"event_id": "E998", "timestamp": "14:00:11", "research_group": "G02",
                       "job_type": "quantum", "duration_sec": 30, "status": "completed", "cpu_pct": 50})
        events.append({"event_id": "E999", "timestamp": "14:00:12", "research_group": "G01",
                       "job_type": "ml", "duration_sec": -4, "status": "done"})

    for ev in events:
        md = producer.send(TOPIC, key=ev.get("research_group", "UNKNOWN"), value=ev).get(timeout=10)
        print(f"[producer] {ev['event_id']} -> partition {md.partition}, offset {md.offset}")
        if args.rate > 0:
            time.sleep(1.0 / args.rate)

    producer.flush()
    producer.close()
    print(f"[producer] done, {len(events)} events sent to '{TOPIC}'")


if __name__ == "__main__":
    main()

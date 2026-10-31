import argparse
import glob
import json
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer

from common import BOOTSTRAP, DLQ_TOPIC, JOB_TYPES, RESULTS_DIR, TOPIC, validate_event


def empty_state():
    return {
        "valid": 0, "invalid": 0, "partitions": {},
        "job": {t: {"completed": 0, "failed": 0, "dur_sum": 0, "n": 0} for t in JOB_TYPES},
        "group": {},
    }


def update(state, ev):
    j = state["job"][ev["job_type"]]
    j[ev["status"]] += 1
    j["dur_sum"] += ev["duration_sec"]
    j["n"] += 1
    g = state["group"].setdefault(ev["research_group"], {"cpu_sum": 0, "n": 0})
    g["cpu_sum"] += ev["cpu_pct"]
    g["n"] += 1


def print_tables(state, title):
    print(f"\n===== {title} =====")
    print(f"valid events: {state['valid']}   invalid (sent to DLQ): {state['invalid']}")
    tot_c = sum(v["completed"] for v in state["job"].values())
    tot_f = sum(v["failed"] for v in state["job"].values())
    print(f"completed jobs: {tot_c}   failed jobs: {tot_f}\n")
    print(f"{'Job type':<12}{'Completed':>10}{'Failed':>8}{'Avg duration':>14}")
    for t in ("ml", "image", "text", "simulation"):
        v = state["job"][t]
        avg = v["dur_sum"] / v["n"] if v["n"] else float("nan")
        print(f"{t:<12}{v['completed']:>10}{v['failed']:>8}{avg:>14.2f}")
    print(f"\n{'Group':<8}{'Events':>8}{'Avg CPU %':>11}")
    for gname in sorted(state["group"]):
        g = state["group"][gname]
        print(f"{gname:<8}{g['n']:>8}{g['cpu_sum'] / g['n']:>11.2f}")


def merge():
    total = empty_state()
    files = sorted(glob.glob(str(RESULTS_DIR / "consumer_*.json")))
    for f in files:
        s = json.load(open(f))
        total["valid"] += s["valid"]
        total["invalid"] += s["invalid"]
        for t, v in s["job"].items():
            for k in v:
                total["job"][t][k] += v[k]
        for gname, g in s["group"].items():
            tg = total["group"].setdefault(gname, {"cpu_sum": 0, "n": 0})
            tg["cpu_sum"] += g["cpu_sum"]
            tg["n"] += g["n"]
    print_tables(total, f"MERGED aggregation layer ({len(files)} consumer files)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--group", default="research-analytics")
    ap.add_argument("--name", default="c1")
    ap.add_argument("--idle", type=int, default=60, help="stop after N idle seconds")
    ap.add_argument("--merge", action="store_true")
    args = ap.parse_args()
    if args.merge:
        return merge()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        client_id=f"research-consumer-{args.name}",
        auto_offset_reset="earliest",     
        enable_auto_commit=True,
        value_deserializer=lambda b: b.decode("utf-8"),
        consumer_timeout_ms=args.idle * 1000,
    )
    dlq = KafkaProducer(bootstrap_servers=args.bootstrap,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    state = empty_state()
    print(f"[{args.name}] waiting for messages on '{TOPIC}' (group '{args.group}') ...")
    for msg in consumer:
        try:
            ev = json.loads(msg.value)
            ok, errors = validate_event(ev)
        except json.JSONDecodeError as exc:
            ev, ok, errors = msg.value, False, [f"invalid JSON: {exc}"]
        state["partitions"].setdefault(str(msg.partition), 0)
        state["partitions"][str(msg.partition)] += 1
        if ok:
            state["valid"] += 1
            update(state, ev)
            print(f"[{args.name}] p{msg.partition} off{msg.offset} VALID   {ev['event_id']} "
                  f"{ev['research_group']} {ev['job_type']:<10} {ev['status']}")
        else:
            state["invalid"] += 1
            dlq.send(DLQ_TOPIC, {"raw": ev, "errors": errors,
                                 "partition": msg.partition, "offset": msg.offset})
            print(f"[{args.name}] p{msg.partition} off{msg.offset} INVALID -> DLQ: {errors}")

    dlq.flush()
    consumer.close()
    print(f"\n[{args.name}] messages per partition: {state['partitions']}")
    print_tables(state, f"consumer {args.name}")
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"consumer_{args.name}.json"
    json.dump(state, open(out, "w"), indent=2)
    print(f"\npartial aggregates saved to {out}")


if __name__ == "__main__":
    main()

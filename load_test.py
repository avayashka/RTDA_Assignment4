"""
load_test.py - Part E: throughput / latency under synthetic load.

Two modes:
  inproc : producer thread -> queue.Queue (stands in for the broker) -> consumer thread.
           No Kafka needed. Used in the notebook.
  kafka  : producer -> Kafka topic `research-events-load` -> consumer (kafka-python).
           Needs the broker from docker-compose.yml.

The consumer does the real per-event work of the pipeline:
JSON decode -> schema validation -> 5-second tumbling-window update.

Latency  = time the consumer finished processing an event - time the producer created it.
Throughput = events processed / (last processed time - first send time).
CPU %    = CPU used by this Python process during the level (psutil; 100 % = one full core).

Run from the project root:
    python src/load_test.py --mode inproc
    python src/load_test.py --mode kafka
"""
from __future__ import annotations

import argparse
import json
import queue
import random
import statistics
import threading
import time
from collections import defaultdict

import psutil

from common import BOOTSTRAP, JOB_TYPES, STUDENT_SEED, validate_event

_SENTINEL = object()


def make_event(i: int, rng: random.Random) -> dict:
    sec = i % 86400
    return {
        "event_id": f"L{i:07d}",
        "timestamp": f"{14 + sec // 3600 % 10:02d}:{sec // 60 % 60:02d}:{sec % 60:02d}",
        "research_group": rng.choice(("G01", "G02", "G03")),
        "job_type": rng.choice(JOB_TYPES),
        "duration_sec": rng.randint(10, 120),
        "status": "failed" if rng.random() < 0.15 else "completed",
        "cpu_pct": rng.randint(30, 99),
    }


class WindowState:
    """Minimal 5-s tumbling-window aggregator used as the 'processing' step."""

    def __init__(self):
        self.win = defaultdict(lambda: [0, 0, 0.0])  # count, failed, cpu_sum

    def update(self, ev: dict) -> None:
        h, m, s = map(int, ev["timestamp"].split(":"))
        key = (h * 3600 + m * 60 + s) // 5 * 5
        w = self.win[key]
        w[0] += 1
        w[1] += ev["status"] == "failed"
        w[2] += ev["cpu_pct"]


def _process(raw: bytes, state: WindowState) -> bool:
    ev = json.loads(raw)
    ok, _ = validate_event(ev)
    if ok:
        state.update(ev)
    return ok


def _paced_send(rate: int, duration_s: float, send_fn, rng) -> int:
    """Send `rate` events/s for `duration_s` seconds (catch-up pacing, ~1 ms sleeps)."""
    total = int(rate * duration_s)
    start = time.perf_counter()
    sent = 0
    while sent < total:
        due = min(total, int((time.perf_counter() - start) * rate) + 1)
        while sent < due:
            ev = make_event(sent, rng)
            send_fn(ev)
            sent += 1
        time.sleep(0.001)
    return sent


def _summary(rate, sent, lat_ms, t_first_send, t_last_done, cpu_pct, mode, batch):
    elapsed = max(1e-9, t_last_done - t_first_send)
    thr = len(lat_ms) / elapsed
    lat_sorted = sorted(lat_ms) or [float("nan")]
    p95 = lat_sorted[int(0.95 * (len(lat_sorted) - 1))]
    return {
        "mode": mode,
        "batch_consumer": batch,
        "input_rate": rate,
        "sent": sent,
        "processed": len(lat_ms),
        "observed_throughput": round(thr, 1),
        "avg_latency_ms": round(statistics.mean(lat_ms), 3) if lat_ms else None,
        "p95_latency_ms": round(p95, 3),
        "max_latency_ms": round(lat_sorted[-1], 3),
        "cpu_pct": round(cpu_pct, 1),
    }


# --------------------------------------------------------------------------
# In-process pipeline (no Kafka)
# --------------------------------------------------------------------------
def run_inproc_level(rate: int, duration_s: float = 10, batch: bool = False,
                     maxsize: int = 100_000, seed: int = STUDENT_SEED) -> dict:
    rng = random.Random(seed)
    q: queue.Queue = queue.Queue(maxsize=maxsize)
    lat_ms: list[float] = []
    state = WindowState()
    t_last = [0.0]

    def consumer():
        while True:
            items = [q.get()]
            if batch:                      # optimisation: drain up to 500 at once
                try:
                    while len(items) < 500:
                        items.append(q.get_nowait())
                except queue.Empty:
                    pass
            for it in items:
                if it is _SENTINEL:
                    return
                t_sent, raw = it
                _process(raw, state)
                now = time.perf_counter()
                lat_ms.append((now - t_sent) * 1000)
                t_last[0] = now

    proc = psutil.Process()
    proc.cpu_percent(None)
    th = threading.Thread(target=consumer, daemon=True)
    th.start()
    t0 = time.perf_counter()
    sent = _paced_send(rate, duration_s,
                       lambda ev: q.put((time.perf_counter(), json.dumps(ev).encode())), rng)
    q.put(_SENTINEL)
    th.join()
    cpu = proc.cpu_percent(None)
    return _summary(rate, sent, lat_ms, t0, t_last[0], cpu, "inproc", batch)


def run_inproc_capacity(n_events: int = 200_000, batch: bool = False) -> dict:
    """Push events as fast as possible to find the maximum sustainable throughput."""
    rng = random.Random(STUDENT_SEED)
    payloads = [json.dumps(make_event(i, rng)).encode() for i in range(n_events)]
    q: queue.Queue = queue.Queue(maxsize=10_000)
    state = WindowState()
    done = [0, 0.0]

    def consumer():
        while True:
            items = [q.get()]
            if batch:
                try:
                    while len(items) < 500:
                        items.append(q.get_nowait())
                except queue.Empty:
                    pass
            for it in items:
                if it is _SENTINEL:
                    return
                _process(it, state)
                done[0] += 1
            done[1] = time.perf_counter()

    th = threading.Thread(target=consumer, daemon=True)
    proc = psutil.Process()
    proc.cpu_percent(None)
    th.start()
    t0 = time.perf_counter()
    for p in payloads:
        q.put(p)
    q.put(_SENTINEL)
    th.join()
    return {"mode": "inproc-capacity", "batch_consumer": batch, "events": done[0],
            "max_throughput": round(done[0] / (done[1] - t0), 1),
            "cpu_pct": round(proc.cpu_percent(None), 1)}


# --------------------------------------------------------------------------
# Kafka pipeline (needs a running broker)
# --------------------------------------------------------------------------
def run_kafka_level(rate: int, duration_s: float = 10, topic: str = "research-events-load",
                    bootstrap: str = BOOTSTRAP) -> dict:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    admin.close()

    rng = random.Random(STUDENT_SEED)
    run_id = f"{rate}-{time.time_ns()}"
    lat_ms: list[float] = []
    state = WindowState()
    t_last = [0.0]
    expected = int(rate * duration_s)
    ready = threading.Event()

    def consumer():
        c = KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id=f"load-{run_id}",
                          auto_offset_reset="latest", enable_auto_commit=True,
                          consumer_timeout_ms=5000)
        c.poll(timeout_ms=1000)            # join group / get partition assignment
        ready.set()
        got = 0
        for msg in c:
            env = json.loads(msg.value)
            if env.get("run") != run_id:
                continue
            _process(json.dumps(env["ev"]).encode(), state)
            now = time.time()
            lat_ms.append((now - env["t"]) * 1000)
            t_last[0] = now
            got += 1
            if got >= expected:
                break
        c.close()

    th = threading.Thread(target=consumer, daemon=True)
    th.start()
    ready.wait(15)
    time.sleep(1)
    prod = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=5, acks=1,
                         key_serializer=str.encode)
    proc = psutil.Process()
    proc.cpu_percent(None)
    t0 = time.time()
    sent = _paced_send(rate, duration_s,
                       lambda ev: prod.send(topic, key=ev["research_group"],
                                            value=json.dumps({"run": run_id, "t": time.time(),
                                                              "ev": ev}).encode()), rng)
    prod.flush()
    th.join(timeout=duration_s + 30)
    cpu = proc.cpu_percent(None)
    prod.close()
    return _summary(rate, sent, lat_ms, t0, t_last[0] or time.time(), cpu, "kafka", False)


def bottleneck_label(r: dict) -> str:
    if r["processed"] < 0.98 * r["sent"] or r["observed_throughput"] < 0.95 * r["input_rate"]:
        return "consumer cannot keep up (single Python thread / GIL)"
    if r["cpu_pct"] >= 85:
        return "CPU of the single process is close to one core"
    if r["mode"] == "kafka":
        return "none - broker round trip and producer batching (linger.ms) dominate latency"
    return "none - limited by input rate; consumer mostly idle"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["inproc", "kafka"], default="inproc")
    ap.add_argument("--rates", default="10,100,500")
    ap.add_argument("--duration", type=float, default=10)
    args = ap.parse_args()
    rates = [int(x) for x in args.rates.split(",")]
    for r in rates:
        res = run_inproc_level(r, args.duration) if args.mode == "inproc" else run_kafka_level(r, args.duration)
        res["bottleneck"] = bottleneck_label(res)
        print(json.dumps(res))

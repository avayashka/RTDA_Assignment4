"""
common.py - shared schema, data and helpers for Assignment 4
(AITU Research Event Analytics with Kafka and Spark).

Used by: Assignment4.ipynb, kafka_producer.py, kafka_consumer.py,
         spark_streaming_job.py, load_test.py

Only the Python standard library is used here, so the Kafka scripts
work without pandas/numpy.
"""
from __future__ import annotations

import csv
import random
import statistics
from datetime import datetime, timedelta
from pathlib import Path

# --------------------------------------------------------------------------
# Paths and individual variant
# --------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"

# Individual variant: the random seed that generates YOUR extended stream.
# Put your own student ID here -> your synthetic data (and every result
# derived from it) is unique to you. Write the same value in the report.
STUDENT_SEED = 240791

EVENT_DATE = "2026-10-31"          # the supplied events only have HH:MM:SS
TOPIC = "research-events"          # main Kafka topic
DLQ_TOPIC = "research-events-dlq"  # dead-letter topic for invalid events
NUM_PARTITIONS = 3                 # one partition per research group
BOOTSTRAP = "localhost:9092"

# --------------------------------------------------------------------------
# Stream schema (from the assignment)
# --------------------------------------------------------------------------
FIELDS = ["event_id", "timestamp", "research_group", "job_type",
          "duration_sec", "status", "cpu_pct"]
JOB_TYPES = ("simulation", "ml", "image", "text")
STATUSES = ("completed", "failed")

# --------------------------------------------------------------------------
# The 10 supplied events (exactly as in the assignment table)
# --------------------------------------------------------------------------
INITIAL_EVENTS = [
    {"event_id": "E001", "timestamp": "14:00:01", "research_group": "G01", "job_type": "ml",         "duration_sec": 42,  "status": "completed", "cpu_pct": 61},
    {"event_id": "E002", "timestamp": "14:00:02", "research_group": "G02", "job_type": "image",      "duration_sec": 75,  "status": "completed", "cpu_pct": 74},
    {"event_id": "E003", "timestamp": "14:00:03", "research_group": "G01", "job_type": "text",       "duration_sec": 18,  "status": "completed", "cpu_pct": 38},
    {"event_id": "E004", "timestamp": "14:00:04", "research_group": "G03", "job_type": "simulation", "duration_sec": 95,  "status": "failed",    "cpu_pct": 92},
    {"event_id": "E005", "timestamp": "14:00:05", "research_group": "G02", "job_type": "ml",         "duration_sec": 51,  "status": "completed", "cpu_pct": 68},
    {"event_id": "E006", "timestamp": "14:00:06", "research_group": "G01", "job_type": "image",      "duration_sec": 88,  "status": "completed", "cpu_pct": 81},
    {"event_id": "E007", "timestamp": "14:00:07", "research_group": "G03", "job_type": "ml",         "duration_sec": 64,  "status": "failed",    "cpu_pct": 89},
    {"event_id": "E008", "timestamp": "14:00:08", "research_group": "G02", "job_type": "text",       "duration_sec": 22,  "status": "completed", "cpu_pct": 44},
    {"event_id": "E009", "timestamp": "14:00:09", "research_group": "G01", "job_type": "simulation", "duration_sec": 120, "status": "completed", "cpu_pct": 94},
    {"event_id": "E010", "timestamp": "14:00:10", "research_group": "G03", "job_type": "image",      "duration_sec": 70,  "status": "completed", "cpu_pct": 78},
]


# --------------------------------------------------------------------------
# Schema validation
# --------------------------------------------------------------------------
def validate_event(ev: dict) -> tuple[bool, list[str]]:
    """Return (is_valid, list_of_errors) for one event dict."""
    errors = []
    if not isinstance(ev, dict):
        return False, ["event is not a JSON object"]
    missing = [f for f in FIELDS if f not in ev]
    if missing:
        errors.append(f"missing fields: {missing}")
    extra = [f for f in ev if f not in FIELDS and f != "arrival_seq"]
    if extra:
        errors.append(f"unexpected fields: {extra}")
    if missing:
        return False, errors

    if not (isinstance(ev["event_id"], str) and ev["event_id"].startswith("E")):
        errors.append("event_id must be a string like 'E001'")
    try:
        datetime.strptime(str(ev["timestamp"]), "%H:%M:%S")
    except ValueError:
        errors.append("timestamp must be HH:MM:SS")
    if not (isinstance(ev["research_group"], str) and ev["research_group"].startswith("G")):
        errors.append("research_group must look like 'G01'")
    if ev["job_type"] not in JOB_TYPES:
        errors.append(f"job_type must be one of {JOB_TYPES}")
    if ev["status"] not in STATUSES:
        errors.append(f"status must be one of {STATUSES}")
    if not (isinstance(ev["duration_sec"], (int, float)) and not isinstance(ev["duration_sec"], bool)
            and ev["duration_sec"] > 0):
        errors.append("duration_sec must be a positive number")
    if not (isinstance(ev["cpu_pct"], (int, float)) and not isinstance(ev["cpu_pct"], bool)
            and 0 <= ev["cpu_pct"] <= 100):
        errors.append("cpu_pct must be a number in [0, 100]")
    return (len(errors) == 0), errors


def to_datetime(hms: str) -> datetime:
    """'14:00:05' -> datetime(2026-10-31 14:00:05)."""
    return datetime.strptime(f"{EVENT_DATE} {hms}", "%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------
# Extended synthetic stream (teacher: Part C must not stop at 10 events)
# --------------------------------------------------------------------------
def generate_extended_stream(end_time: str = "14:02:00",
                             seed: int = STUDENT_SEED,
                             mean_gap_sec: float = 1.0,
                             late_fraction: float = 0.05,
                             max_lateness_sec: int = 3) -> list[dict]:
    """
    Continue the stream after E010 with synthetic events whose statistical
    profile is ESTIMATED FROM THE 10 SUPPLIED EVENTS. This is simulation of
    more traffic, not forecasting of specific future values.

    - arrivals: Poisson process (exponential gaps, mean `mean_gap_sec`)
    - job_type mix: frequencies of the supplied events (ml .3, image .3, text .2, simulation .2)
    - duration: per-job-type mean of supplied events, +-15 % noise
    - cpu_pct: linear regression cpu ~ duration fitted on supplied events + noise
    - status: failure probability grows when CPU is high (supplied failures had CPU 89-92)
    - ~5 % of events arrive out of order (event time 1..3 s older than arrival)
      to exercise event-time windowing.
    Returns the 10 supplied events followed by the generated ones, in ARRIVAL order.
    """
    rng = random.Random(seed)

    # --- profile estimated from the supplied data --------------------------
    types = [e["job_type"] for e in INITIAL_EVENTS]
    type_weights = {t: types.count(t) / len(types) for t in JOB_TYPES}
    dur_mean = {t: statistics.mean(e["duration_sec"] for e in INITIAL_EVENTS if e["job_type"] == t)
                for t in JOB_TYPES}
    slope, intercept = statistics.linear_regression(
        [e["duration_sec"] for e in INITIAL_EVENTS], [e["cpu_pct"] for e in INITIAL_EVENTS])
    groups = sorted({e["research_group"] for e in INITIAL_EVENTS})

    events = [dict(e, arrival_seq=i + 1) for i, e in enumerate(INITIAL_EVENTS)]
    t = to_datetime(INITIAL_EVENTS[-1]["timestamp"])
    t_end = to_datetime(end_time)
    n = len(INITIAL_EVENTS)

    while True:
        t = t + timedelta(seconds=rng.expovariate(1.0 / mean_gap_sec))
        if t >= t_end:
            break
        n += 1
        job = rng.choices(list(type_weights), weights=list(type_weights.values()))[0]
        duration = max(5, round(rng.gauss(dur_mean[job], 0.15 * dur_mean[job])))
        cpu = intercept + slope * duration + rng.gauss(0, 6)
        cpu = int(min(99, max(10, round(cpu))))
        p_fail = 0.05 + (0.40 if cpu >= 85 else 0.0)
        status = "failed" if rng.random() < p_fail else "completed"

        event_time = t.replace(microsecond=0)
        if rng.random() < late_fraction:          # out-of-order / late event
            event_time -= timedelta(seconds=rng.randint(1, max_lateness_sec))

        events.append({
            "event_id": f"E{n:03d}",
            "timestamp": event_time.strftime("%H:%M:%S"),
            "research_group": rng.choice(groups),
            "job_type": job,
            "duration_sec": duration,
            "status": status,
            "cpu_pct": cpu,
            "arrival_seq": n,
        })
    return events


# --------------------------------------------------------------------------
# CSV helpers
# --------------------------------------------------------------------------
def save_events_csv(events: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = FIELDS + (["arrival_seq"] if "arrival_seq" in events[0] else [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(events)


def load_events_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["duration_sec"] = int(r["duration_sec"])
        r["cpu_pct"] = int(r["cpu_pct"])
        if "arrival_seq" in r:
            r["arrival_seq"] = int(r["arrival_seq"])
    return rows

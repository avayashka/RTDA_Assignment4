import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(15, 7.2))
ax.set_xlim(0, 150); ax.set_ylim(0, 72); ax.axis("off")

def box(x, y, w, h, text, fc="#EEF3FA", ec="#4C78A8", fs=9.5, bold=False, ls="-"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2",
                                fc=fc, ec=ec, lw=1.4, ls=ls))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs,
            fontweight="bold" if bold else "normal", wrap=True)

def group(x, y, w, h, title, fc):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.6,rounding_size=2",
                                fc=fc, ec="#999999", lw=1, ls="--"))
    ax.text(x + w / 2, y + h - 2.2, title, ha="center", va="center", fontsize=10, fontweight="bold")

def arrow(x1, y1, x2, y2, style="-", color="#333333", text=None, rad=0.0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=13,
                                 color=color, lw=1.3, ls=style, connectionstyle=f"arc3,rad={rad}"))
    if text:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 1.3, text, fontsize=8, ha="center", color=color)

# Producers
group(1, 20, 22, 46, "Producers", "#F7F7F7")
for i, g in enumerate(["G01", "G02", "G03"]):
    box(3.5, 52 - i * 13, 17, 8, f"Producer {g}\nkey = {g}\nacks=all, retries")

# Kafka
group(30, 14, 34, 52, "Kafka broker (KRaft)\ntopic: research-events", "#FFF8E6")
for i in range(3):
    box(36, 47 - i * 11, 22, 7, f"Partition {i}\noffsets 0..n", fc="#FFFFFF", ec="#F58518")
box(36, 16.5, 22, 7, "research-events-dlq\n(invalid events)", fc="#FDECEC", ec="#E45756")

# Consumer group
group(71, 20, 30, 46, "Consumer group\nresearch-analytics", "#EEF7F1")
for i in range(3):
    box(74, 48 - i * 12.5, 24, 8.5, f"Consumer c{i+1}\nPython processor:\nvalidate + 5-s windows", ec="#54A24B")

# Spark
box(69, 3, 34, 10, "Spark Structured Streaming\nwatermark 10 s, window(5 s) x job_type\n-> count, avg_duration", fc="#F3EEF9", ec="#B279A2")

# Aggregation + monitoring
box(106, 37, 21, 18, "Aggregation layer\n\nmerge partial sums\nby job_type, group,\nwindow -> results/*.csv", fc="#EEF3FA", ec="#4C78A8")
box(130.5, 30, 18.5, 30, "Monitoring\n\nconsumer lag\nthroughput, latency\nfailure-rate alert\nCPU >= 85 % alert\nKafka UI :8080", fc="#FFFBEA", ec="#B8860B")

# arrows producers -> topic
for i in range(3):
    arrow(21, 56 - i * 13, 35.5, 50.5 - i * 11)
# partitions -> consumers
for i in range(3):
    arrow(58.5, 50.5 - i * 11, 73.5, 52.2 - i * 12.5)
# consumers -> dlq
arrow(74, 26.5, 58.5, 20, style="--", color="#E45756", text="invalid", rad=-0.1)
# topic -> spark
arrow(47, 13.5, 68.5, 8, color="#B279A2", rad=0.15)
ax.text(44, 7, "readStream\n(kafka source)", fontsize=8, color="#B279A2", ha="center")
# consumers -> agg
for i in range(3):
    arrow(98.5, 52.2 - i * 12.5, 105.5, 46)
arrow(103.5, 8, 116, 36.5, color="#B279A2", rad=0.2)
arrow(127.5, 46, 130.5, 46)
arrow(64.5, 64, 131, 61, style=":", color="#B8860B", rad=-0.12)
ax.text(118, 66.5, "offsets / lag metrics", fontsize=8, color="#B8860B")

ax.text(75, 70.5, "AITU research-event streaming pipeline (Assignment 4, synthetic)",
        ha="center", fontsize=13, fontweight="bold")
ax.text(1, 1, "Scaling: add partitions + consumers (max useful consumers = partitions).  "
        "Failure: replication factor 3 in production, committed offsets, DLQ, checkpoints in Spark.",
        fontsize=8.5, color="#555555")
import os; plt.savefig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "architecture.png"), dpi=160, bbox_inches="tight")

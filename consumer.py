import json
import os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer
from streamingconfig import *

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------- LOAD STATE ----------------

def load(file, default):
    if os.path.exists(file):
        with open(file, "r") as f:
            return json.load(f)
    return default


def save(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=4)


processed = set(load(PROCESSED_FILE, []))
checkpoint = load(CHECKPOINT_FILE, {})

# ---------------- KAFKA CONSUMER ----------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="logs-consumer-group",   # ✅ IMPORTANT FIX
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

batch = []

# ---------------- WRITE PARQUET ----------------

def write_parquet(rows):
    if not rows:
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    for date, group in df.groupby("date"):
        path = os.path.join(OUTPUT_DIR, f"date={date}")
        os.makedirs(path, exist_ok=True)

        file = os.path.join(
            path,
            f"part-{datetime.now().timestamp()}.parquet"
        )

        group.to_parquet(file, index=False)
        print("Written:", file)

# ---------------- STREAM PROCESS ----------------

print("Consumer started...")

try:
    for msg in consumer:
        event = msg.value
        event_id = event["event_id"]

        # ✅ IDEMPOTENCY CHECK
        if event_id in processed:
            print("Duplicate skipped:", event_id)
            continue

        processed.add(event_id)
        batch.append(event)

        # checkpoint Kafka offset
        checkpoint[str(msg.partition)] = msg.offset

        # batch write
        if len(batch) >= BATCH_SIZE:
            write_parquet(batch)

            save(PROCESSED_FILE, list(processed))
            save(CHECKPOINT_FILE, checkpoint)

            consumer.commit()   # ✅ IMPORTANT FIX

            batch.clear()

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    # flush remaining records
    write_parquet(batch)

    save(PROCESSED_FILE, list(processed))
    save(CHECKPOINT_FILE, checkpoint)

    consumer.commit()       # final commit
    consumer.close()

    print("Consumer shut down cleanly.")
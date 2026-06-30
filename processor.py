import os
import json
from datetime import datetime

import pandas as pd

from config import *

# Create required directories
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("checkpoints", exist_ok=True)
os.makedirs("processed", exist_ok=True)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=4)


def load_processed():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_processed(processed):
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(processed), f, indent=4)


def write_parquet(rows):
    if not rows:
        return

    df = pd.DataFrame(rows)

    df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    for date, group in df.groupby("date"):
        partition_dir = os.path.join(OUTPUT_DIR, f"date={date}")
        os.makedirs(partition_dir, exist_ok=True)

        filename = os.path.join(
            partition_dir,
            f"part-{datetime.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
        )

        group.to_parquet(filename, index=False)

        print(f"Written: {filename}")


checkpoint = load_checkpoint()
processed = load_processed()

files = sorted(os.listdir(INPUT_DIR))

for file in files:

    if not file.endswith(".jsonl"):
        continue

    path = os.path.join(INPUT_DIR, file)

    print(f"\nProcessing {file}")

    last_processed_line = checkpoint.get(file, 0)

    batch = []

    with open(path, "r") as f:
        lines = f.readlines()

    for line_number, line in enumerate(lines):

        if line_number < last_processed_line:
            continue

        event = json.loads(line)

        event_id = event["event_id"]

        if event_id in processed:
            print(f"Duplicate skipped: {event_id}")
            checkpoint[file] = line_number + 1
            continue

        processed.add(event_id)

        batch.append(event)

        checkpoint[file] = line_number + 1

        if len(batch) >= BATCH_SIZE:
            write_parquet(batch)
            batch.clear()

            save_checkpoint(checkpoint)
            save_processed(processed)

    if batch:
        write_parquet(batch)

    save_checkpoint(checkpoint)
    save_processed(processed)

print("\nStreaming completed successfully.")
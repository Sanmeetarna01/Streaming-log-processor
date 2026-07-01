# Streaming Log Processor

## Overview

This project simulates a streaming data pipeline using Python.

Features:

- Reads newline-delimited JSON (JSONL) log files.
- Processes logs in batches.
- Writes output as partitioned Parquet files.
- Supports checkpointing.
- Implements exactly-once semantics using event IDs.

---

## Project Structure

```
streaming-log-processor/
│
├── input_logs/
├── output/
├── checkpoints/
├── processed/
├── config.py
├── processor.py
├── requirements.txt
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Run

```bash
python processor.py
```

---

## Output

```
output/

    date=2026-06-30/

        part-xxxxxxxx.parquet
```

---

## Checkpoint

Checkpoint file:

```
checkpoints/checkpoint.json
```

Example:

```json
{
    "logs1.jsonl": 5,
    "logs2.jsonl": 5
}
```

---

## Processed IDs

```
processed/processed_ids.json
```

Example:

```json
[
    "101",
    "102",
    "103",
    "104"
]
```

---

## Exactly-Once Semantics

Each event has a unique `event_id`.

Before processing:

- Check if `event_id` already exists.
- Skip duplicate events.
- Save processed IDs.
- Save checkpoints after each batch.

If the application restarts, processing resumes from the last checkpoint without duplicating events.


--------------------------------------

Your processor.py is doing 4 main things:

Reads JSONL log files (like a stream)
Processes events in batches
Prevents duplicates (exactly-once semantics)
Writes output as partitioned Parquet files
Saves state so it can resume after crash (checkpointing)


--------------------------------------


Exactly-once check
if event_id in processed:
    continue
Meaning:

If event already seen → skip it

👉 This is the core of idempotency
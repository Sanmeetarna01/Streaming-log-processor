# Directory containing input JSONL log files

INPUT_DIR = "input_logs"

# Output directory for partitioned Parquet files
OUTPUT_DIR = "output"

# Stores the last processed line number for each file
CHECKPOINT_FILE = "checkpoints/checkpoint.json"

# Stores processed event IDs for exactly-once semantics
PROCESSED_FILE = "processed/processed_ids.json"

# Number of events processed before writing to Parquet
BATCH_SIZE = 5
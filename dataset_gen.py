import argparse
import os
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


REGIONS = ["EU", "US", "ASIA", "AFRICA"]
EVENT_TYPES = ["click", "view", "purchase", "login", "logout"]


def generate_rows(num_rows: int, seed: int = 42):
    random.seed(seed)

    start_date = datetime(2026, 1, 1)

    timestamps = []
    user_ids = []
    regions = []
    event_types = []
    values = []

    for _ in range(num_rows):
        random_days = random.randint(0, 30)
        random_seconds = random.randint(0, 86400)

        ts = start_date + timedelta(days=random_days, seconds=random_seconds)

        timestamps.append(ts)
        user_ids.append(random.randint(1, 1_000_000))
        regions.append(random.choice(REGIONS))
        event_types.append(random.choice(EVENT_TYPES))
        values.append(random.random() * 100)

    table = pa.table({
        "ts": pa.array(timestamps, type=pa.timestamp("ms")),
        "user_id": pa.array(user_ids, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "event_type": pa.array(event_types, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

    return table


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    table = generate_rows(args.rows, args.seed)

    pq.write_table(
        table,
        args.output,
        compression="snappy"
    )

    print(f"Dataset generated successfully")
    print(f"Rows: {args.rows}")
    print(f"Output: {args.output}")
    print(f"Size: {os.path.getsize(args.output) / 1_000_000:.2f} MB")


if __name__ == "__main__":
    main()

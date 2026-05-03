import argparse
import os
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


REGIONS = ["Zurich", "Geneva", "Lausanne", "Basel", "Bern"]

EVENT_TYPES = [
    "order_placed",
    "restaurant_accepted",
    "courier_assigned",
    "courier_pickup",
    "delivery_completed",
    "order_cancelled",
]

ROW_COUNTS = {
    "S": 5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}


def generate_batch(num_rows: int, seed: int) -> pa.Table:
    random.seed(seed)

    start_date = datetime(2026, 1, 1)

    timestamps = []
    user_ids = []
    regions = []
    event_types = []
    values = []

    for _ in range(num_rows):
        ts = start_date + timedelta(
            days=random.randint(0, 30),
            seconds=random.randint(0, 86_400),
        )

        timestamps.append(ts)
        user_ids.append(random.randint(1, 1_000_000))
        regions.append(random.choice(REGIONS))
        event_types.append(random.choice(EVENT_TYPES))
        values.append(round(random.uniform(5, 100), 2))

    return pa.table({
        "ts": pa.array(timestamps, type=pa.timestamp("ms")),
        "user_id": pa.array(user_ids, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "event_type": pa.array(event_types, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--batch-size", type=int, default=500_000)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    total_rows = ROW_COUNTS[args.size]
    os.makedirs(args.output_dir, exist_ok=True)

    output_path = os.path.join(args.output_dir, f"dataset_{args.size}.parquet")

    writer = None
    written_rows = 0
    batch_id = 0

    while written_rows < total_rows:
        rows_this_batch = min(args.batch_size, total_rows - written_rows)

        table = generate_batch(
            num_rows=rows_this_batch,
            seed=args.seed + batch_id,
        )

        if writer is None:
            writer = pq.ParquetWriter(
                output_path,
                table.schema,
                compression="snappy",
            )

        writer.write_table(table)

        written_rows += rows_this_batch
        batch_id += 1

        print(f"Wrote {written_rows:,}/{total_rows:,} rows")

    if writer:
        writer.close()

    size_gb = os.path.getsize(output_path) / 1e9

    print("\nDataset generated successfully")
    print("Concept: Uber Eats-like event logs")
    print(f"Size label: {args.size}")
    print(f"Rows: {written_rows:,}")
    print(f"Stored Parquet size: {size_gb:.2f} GB")
    print(f"Output: {os.path.abspath(output_path)}")


if __name__ == "__main__":
    main()
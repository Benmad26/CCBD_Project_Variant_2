import argparse
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Available regions in the dataset (Swiss cities)
REGIONS = np.array([
    "Zurich",
    "Geneva",
    "Lausanne",
    "Basel",
    "Bern",
])

# Event types simulating a food delivery service (Uber Eats-like)
EVENT_TYPES = np.array([
    "order_placed",
    "restaurant_accepted",
    "courier_assigned",
    "courier_pickup",
    "delivery_completed",
    "order_cancelled",
])

# Number of rows per dataset size
# small, medium, large
ROW_COUNTS = {
    "S": 5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}

# ===== BATCH GENERATION =====
# Generates one batch of rows using NumPy (faster than a Python loop)
# Each column has a fixed type (int, string, float, timestamp)
def generate_batch(num_rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)

    start_date = np.datetime64("2026-01-01T00:00:00", "ms")

    # Generate random days and seconds, combine into timestamps
    days = rng.integers(0, 31, size=num_rows)
    seconds = rng.integers(0, 86_401, size=num_rows)
    timestamps = (
        start_date
        + days.astype("timedelta64[D]")
        + seconds.astype("timedelta64[s]")
    )

    user_ids = rng.integers(
        1,
        1_000_001,
        size=num_rows,
        dtype=np.int64,
    )

    region_indices = rng.integers(
        0,
        len(REGIONS),
        size=num_rows,
    )

    event_indices = rng.integers(
        0,
        len(EVENT_TYPES),
        size=num_rows,
    )

    # Pick random regions
    regions = REGIONS[region_indices]
    event_types = EVENT_TYPES[event_indices]

    values = np.round(
        rng.uniform(5, 100, size=num_rows),
        2,
    )

    return pa.table({
        "ts": pa.array(timestamps, type=pa.timestamp("ms")),
        "user_id": pa.array(user_ids, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "event_type": pa.array(event_types, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

# ===== MAIN =====
# Generates the full dataset in batches and writes it to a Parquet file
# Each batch uses a different seed to avoid identical data across batches

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--size",
        choices=["S", "M", "L"],
        default="S",
    )

    parser.add_argument(
        "--output-dir",
        default="data2",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=500_000,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    args = parser.parse_args()

    total_rows = ROW_COUNTS[args.size]

    os.makedirs(args.output_dir, exist_ok=True)

    output_path = os.path.join(
        args.output_dir,
        f"dataset_{args.size}.parquet",
    )

    writer = None
    written_rows = 0
    batch_id = 0

    # Write the dataset batch by batch to avoid loading everything into RAM
    while written_rows < total_rows:
        rows_this_batch = min(
            args.batch_size,
            total_rows - written_rows,
        )

        table = generate_batch(
            num_rows=rows_this_batch,
            seed=args.seed + batch_id,
        )
        
        # Initialize the Parquet writer on the first batch
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

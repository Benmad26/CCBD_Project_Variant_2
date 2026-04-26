import csv
import os
import time
from datetime import datetime

import pyarrow.dataset as ds
import pyarrow.compute as pc


CURATED_DIR = "data/curated"
RESULTS_FILE = "results.csv"

LAYOUTS = {
    "flat": "data/curated/flat",
    "by_date": "data/curated/by_date",
    "by_region": "data/curated/by_region",
    "by_date_region": "data/curated/by_date_region",
}


def list_files_and_size(path):
    start = time.perf_counter()

    file_count = 0
    total_size = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                file_count += 1
                full_path = os.path.join(root, file)
                total_size += os.path.getsize(full_path)

    end = time.perf_counter()

    return file_count, total_size, end - start


def run_query(path, query_type):
    dataset = ds.dataset(path, format="parquet", partitioning="hive")

    if query_type == "selective":
        filter_expr = (
            (pc.field("region") == "EU") &
            (pc.field("ts") >= datetime(2026, 1, 5)) &
            (pc.field("ts") <= datetime(2026, 1, 10))
        )

    elif query_type == "broad":
        filter_expr = (
            (pc.field("ts") >= datetime(2026, 1, 1)) &
            (pc.field("ts") <= datetime(2026, 1, 31))
        )

    else:
        raise ValueError("Unknown query type")

    start = time.perf_counter()

    table = dataset.to_table(
        filter=filter_expr,
        columns=["event_type", "value"]
    )

    result = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean"),
    ])

    end = time.perf_counter()

    return end - start, result.num_rows


def main():
    rows = []

    for layout_name, path in LAYOUTS.items():
        print(f"\nBenchmarking layout: {layout_name}")

        file_count, total_size, listing_time = list_files_and_size(path)

        for query_type in ["selective", "broad"]:
            print(f"  Running query: {query_type}")

            query_time, result_rows = run_query(path, query_type)

            rows.append({
                "layout": layout_name,
                "query_type": query_type,
                "file_count": file_count,
                "total_size_mb": total_size / 1_000_000,
                "listing_time_sec": listing_time,
                "query_time_sec": query_time,
                "result_rows": result_rows,
            })

    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "layout",
                "query_type",
                "file_count",
                "total_size_mb",
                "listing_time_sec",
                "query_time_sec",
                "result_rows",
            ]
        )

        writer.writeheader()
        writer.writerows(rows)

    print("\nBenchmark finished!")
    print(f"Results saved to: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
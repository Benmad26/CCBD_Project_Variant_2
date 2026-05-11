import os
import shutil
import argparse

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


SIZES = ["S", "M", "L"]

BASE_INPUT_DIR = "data"
BASE_OUTPUT_DIR = "data/curated"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=1_000_000)
    return parser.parse_args()


def clean_output():
    if os.path.exists(BASE_OUTPUT_DIR):
        shutil.rmtree(BASE_OUTPUT_DIR)

    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)


def add_date_column(table):
    date_col = pc.strftime(table["ts"], format="%Y-%m-%d")
    return table.append_column("date", date_col)


def copy_flat(input_file, output_dir):
    flat_dir = os.path.join(output_dir, "flat")
    os.makedirs(flat_dir, exist_ok=True)

    output_file = os.path.join(flat_dir, "data.parquet")
    shutil.copy2(input_file, output_file)


def write_partitioned_batches(input_file, output_dir, batch_size):
    parquet_file = pq.ParquetFile(input_file)

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
        table = pa.Table.from_batches([batch])
        table = add_date_column(table)

        pq.write_to_dataset(
            table,
            root_path=os.path.join(output_dir, "by_date"),
            partition_cols=["date"],
            compression="snappy",
        )

        pq.write_to_dataset(
            table,
            root_path=os.path.join(output_dir, "by_region"),
            partition_cols=["region"],
            compression="snappy",
        )

        print(f"  Batch {i + 1} written")


def process_size(size, batch_size):
    input_file = os.path.join(BASE_INPUT_DIR, f"dataset_{size}.parquet")
    output_dir = os.path.join(BASE_OUTPUT_DIR, size)

    print(f"\nProcessing size {size}...")
    print(f"Input: {input_file}")

    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    print("Copying flat layout...")
    copy_flat(input_file, output_dir)

    print("Writing partitioned layouts...")
    write_partitioned_batches(input_file, output_dir, batch_size)

    print(f"Done for {size} → {output_dir}")


def main():
    args = parse_args()

    print("Cleaning output directory...")
    clean_output()

    for size in SIZES:
        process_size(size, args.batch_size)

    print("\nAll layouts created successfully!")


if __name__ == "__main__":
    main()
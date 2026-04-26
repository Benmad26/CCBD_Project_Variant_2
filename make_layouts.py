import os
import shutil

import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc


INPUT_FILE = "data/events.parquet"
OUTPUT_DIR = "data/curated"


def clean_output():
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)

    os.makedirs(OUTPUT_DIR, exist_ok=True)


def add_date_column(table):
    # Convertit ts en date YYYY-MM-DD
    date_col = pc.strftime(table["ts"], format="%Y-%m-%d")

    return table.append_column("date", date_col)


def write_flat(table):
    path = os.path.join(OUTPUT_DIR, "flat")

    pq.write_to_dataset(
        table,
        root_path=path,
        compression="snappy"
    )


def write_by_date(table):
    path = os.path.join(OUTPUT_DIR, "by_date")

    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=["date"],
        compression="snappy"
    )


def write_by_region(table):
    path = os.path.join(OUTPUT_DIR, "by_region")

    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=["region"],
        compression="snappy"
    )


def write_by_date_region(table):
    path = os.path.join(OUTPUT_DIR, "by_date_region")

    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=["date", "region"],
        compression="snappy"
    )


def main():
    print("Reading input file...")
    table = pq.read_table(INPUT_FILE)

    print("Adding date column...")
    table = add_date_column(table)

    print("Cleaning old output...")
    clean_output()

    print("Writing flat layout...")
    write_flat(table)

    print("Writing by_date layout...")
    write_by_date(table)

    print("Writing by_region layout...")
    write_by_region(table)

    print("Writing by_date_region layout...")
    write_by_date_region(table)

    print("Done!")
    print(f"Layouts created in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
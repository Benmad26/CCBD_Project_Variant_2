import os
import shutil

import pyarrow.parquet as pq
import pyarrow.compute as pc


# Tailles à traiter
SIZES = ["S", "M", "L"]

# Dossier de sortie
BASE_OUTPUT_DIR = "data/curated"


def clean_output():
    """
    Supprime complètement le dossier curated pour repartir propre.
    """
    if os.path.exists(BASE_OUTPUT_DIR):
        shutil.rmtree(BASE_OUTPUT_DIR)

    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)


def add_date_column(table):
    """
    Ajoute une colonne 'date' au format YYYY-MM-DD
    à partir du timestamp 'ts'
    """
    date_col = pc.strftime(table["ts"], format="%Y-%m-%d")
    return table.append_column("date", date_col)


def write_flat(table, output_dir):
    """
    Écrit le dataset sans partition (flat layout)
    """
    pq.write_to_dataset(
        table,
        root_path=os.path.join(output_dir, "flat"),
        compression="snappy"
    )


def write_by_date(table, output_dir):
    """
    Partition par date (date=YYYY-MM-DD)
    """
    pq.write_to_dataset(
        table,
        root_path=os.path.join(output_dir, "by_date"),
        partition_cols=["date"],
        compression="snappy"
    )


def write_by_region(table, output_dir):
    """
    Partition par région (region=Zurich, etc.)
    """
    pq.write_to_dataset(
        table,
        root_path=os.path.join(output_dir, "by_region"),
        partition_cols=["region"],
        compression="snappy"
    )


def process_size(size):
    """
    Traite une taille (S, M ou L)
    """
    input_file = f"data/dataset_{size}.parquet"
    output_dir = os.path.join(BASE_OUTPUT_DIR, size)

    print(f"\nProcessing size {size}...")
    print(f"Reading {input_file}...")

    table = pq.read_table(input_file)

    print("Adding date column...")
    table = add_date_column(table)

    print("Writing flat layout...")
    write_flat(table, output_dir)

    print("Writing by_date layout...")
    write_by_date(table, output_dir)

    print("Writing by_region layout...")
    write_by_region(table, output_dir)

    print(f"Done for {size} → {output_dir}")


def main():
    print("Cleaning output directory...")
    clean_output()

    for size in SIZES:
        process_size(size)

    print("\nAll layouts created successfully!")


if __name__ == "__main__":
    main()
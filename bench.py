import os
import csv
import time
import pyarrow.dataset as ds


# ===== CONFIG =====

BASE_DIR = "data/curated"   # dossier où sont les datasets
SIZES = ["S", "M", "L"]     # tailles à tester
LAYOUTS = ["flat", "by_date", "by_region"]  # layouts à comparer
RESULTS_FILE = "results.csv"  # fichier de sortie


# ===== FILE METRICS =====

def count_files_and_bytes(path):
    # mesure le temps pour parcourir les fichiers
    start = time.time()

    file_count = 0
    total_bytes = 0

    # parcourt tous les fichiers du dossier
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                file_count += 1
                total_bytes += os.path.getsize(os.path.join(root, file))

    listing_time = time.time() - start

    return listing_time, file_count, total_bytes


# ===== DATASET LOADING =====

def load_dataset(path):
    # charge le dataset parquet
    # "hive" permet de lire les partitions (region=..., date=...)
    return ds.dataset(path, format="parquet", partitioning="hive")


# ===== QUERIES =====

def run_selective_query(path):
    # requête avec filtre (rapide si partitionnement utile)
    dataset = load_dataset(path)

    start = time.time()

    table = dataset.to_table(
        filter=(
            (ds.field("region") == "Zurich") &
            (ds.field("date") >= "2026-01-10") &
            (ds.field("date") <= "2026-01-12")
        ),
        columns=["event_type", "value"]  # lire seulement colonnes utiles
    )

    query_time = time.time() - start

    return query_time, table.num_rows


def run_broad_query(path):
    # requête large (scan tout le dataset)
    dataset = load_dataset(path)

    start = time.time()

    table = dataset.to_table(
        columns=["event_type", "value"]
    )

    query_time = time.time() - start

    return query_time, table.num_rows


# ===== MAIN BENCH =====

def main():
    results = []

    print("Starting benchmark...\n")

    # boucle sur tailles et layouts
    for size in SIZES:
        for layout in LAYOUTS:
            path = os.path.join(BASE_DIR, size, layout)

            print(f"=== {size} - {layout} ===")

            if not os.path.exists(path):
                print("Path missing")
                continue

            # 1. listing (nombre fichiers + temps)
            listing_time, file_count, total_bytes = count_files_and_bytes(path)

            # 2. requête selective
            sel_time, sel_rows = run_selective_query(path)

            # 3. requête large
            broad_time, broad_rows = run_broad_query(path)

            # afficher résultats
            print(f"Listing: {listing_time:.4f}s | files: {file_count}")
            print(f"Selective: {sel_time:.4f}s | rows: {sel_rows}")
            print(f"Broad: {broad_time:.4f}s | rows: {broad_rows}")
            print()

            # stocker résultats pour CSV
            results.append({
                "size": size,
                "layout": layout,
                "listing_time_s": listing_time,
                "file_count": file_count,
                "total_size_gb": total_bytes / 1e9,
                "selective_time_s": sel_time,
                "selective_rows": sel_rows,
                "broad_time_s": broad_time,
                "broad_rows": broad_rows,
            })

    # ===== WRITE CSV =====

    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("Results saved to results.csv")


if __name__ == "__main__":
    main()
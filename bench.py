import os
import csv
import time
import argparse
import boto3
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa 
from datetime import datetime


# ===== CONFIG =====

# Dossier local où sont stockés les datasets curated (générés par make_layouts.py)
BASE_DIR = "data/curated"

# Tailles et layouts à benchmarker
SIZES = ["S", "M", "L"]
LAYOUTS = ["flat", "by_date", "by_region"]

# Fichier de sortie du benchmark
RESULTS_FILE = "results.csv"

# Connexion au bucket MinIO 
BUCKET = "ccbd"
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=boto3.session.Config(
        connect_timeout=2,
        retries={"max_attempts": 0}
    )
)

# ===== ARGPARSE =====

# Lance le benchmark avec paramètres personnalisés sans modifier le code
# Exemple : python bench.py --size S --region Geneva --date-start 2026-01-05 --date-end 2026-01-15
def parse_args():
    parser = argparse.ArgumentParser()

    # Taille du dataset à benchmarker (S, M, L ou all pour les 3)
    parser.add_argument("--size", choices=["S", "M", "L", "all"], default="all")

    # Paramètres de la requête sélective
    parser.add_argument("--region", default="Zurich")
    parser.add_argument("--date-start", default="2026-01-10")
    parser.add_argument("--date-end", default="2026-01-12")

    # Nombre de répétitions pour calculer la médiane
    parser.add_argument("--runs", type=int, default=3)

    return parser.parse_args()


# ===== LISTING =====
# Parcourt un dossier local et compte les fichiers Parquet
# Mesure aussi le temps de parcours (listing time)
# Retourne : (temps de listing, nombre de fichiers, taille totale en bytes)

def count_files_and_bytes(path):
    start = time.time()
    file_count = 0
    total_bytes = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                file_count += 1
                total_bytes += os.path.getsize(os.path.join(root, file))

    listing_time = time.time() - start
    return listing_time, file_count, total_bytes


# ===== LISTING S3 =====
# Liste les objets Parquet dans un préfixe S3 (MinIO)
# Utilise un paginator pour gérer les grands nombres d'objets (>1000)
# Mesure aussi le temps de listing côté S3
# Retourne : (temps de listing, nombre d'objets, taille totale en bytes)

def list_s3_objects(prefix):
    start = time.time()
    object_count = 0
    total_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            object_count += 1
            total_bytes += obj["Size"]

    listing_time = time.time() - start
    return listing_time, object_count, total_bytes


# ===== UPLOAD / DOWNLOAD THROUGHPUT =====

# Mesure la vitesse d'upload d'un fichier local vers S3 (en MB/s)
def measure_upload_throughput(local_path, s3_key):
    file_size = os.path.getsize(local_path)
    start = time.time()
    s3.upload_file(local_path, BUCKET, s3_key)
    elapsed = time.time() - start
    throughput = (file_size / 1e6) / elapsed  # MB/s
    return throughput

# Mesure la vitesse de download d'un fichier S3 vers local (en MB/s)
# Un premier download est fait pour s'assurer que le fichier existe
# Le second download est celui qui est mesuré
def measure_download_throughput(s3_key, local_path):
    s3.download_file(BUCKET, s3_key, local_path)
    file_size = os.path.getsize(local_path)
    # re-download to measure (first was just to get size)
    start = time.time()
    s3.download_file(BUCKET, s3_key, local_path)
    elapsed = time.time() - start
    throughput = (file_size / 1e6) / elapsed  # MB/s
    return throughput


# ===== Queries =====

# Requête sélective : filtre sur une région et une plage de dates
# Ne lit pas tout le fichier (partition pruning) si le layout est by_region ou by_date
# Pour le layout flat, filtre sur ts directement (pas de colonne date)
# Calcule ensuite un group by event_type avec count et mean de value
# Retourne : (temps d'exécution, nombre de lignes, résultat groupé)

def run_selective_query(path, region, date_start, date_end, layout):
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    start = time.time()

    if layout == "flat":
        # flat n'a pas de colonne date, on filtre sur ts directement
        ts_start = datetime.strptime(date_start, "%Y-%m-%d")
        ts_end = datetime.strptime(date_end, "%Y-%m-%d")
        filter_expr = (
            (ds.field("region") == region) &
            (ds.field("ts") >= ts_start) &
            (ds.field("ts") <= ts_end)
        )
    
    else:
        filter_expr = (
            (ds.field("region") == region) &
            (ds.field("date") >= date_start) &
            (ds.field("date") <= date_end)
        )

    table = dataset.to_table(
        filter=filter_expr,
        columns=["event_type", "value"]
    )

    grouped = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])
    query_time = time.time() - start
    return query_time, table.num_rows, grouped

# Requête large : scan complet du dataset sans filtre
# Mesure le coût d'une agrégation globale sur tous les layouts
# Retourne : (temps d'exécution, nombre de lignes, résultat groupé)

def run_broad_query(path):
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    start = time.time()
    table = dataset.to_table(
        columns=["event_type", "value"]
    )

    grouped = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])
    query_time = time.time() - start
    return query_time, table.num_rows, grouped


# ===== MEDIAN =====
# Calcule la médiane d'une liste de valeurs
# Permet d'obtenir une mesure de temps représentative
# évite l'influence des valeurs aberrantes (ex: premier run plus lent)

def median(values):
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]


# ===== MAIN BENCH =====
# Gère toutes les mesures pour chaque combinaison de taille et layout
# Sauvegarde les résultats dans results.csv

def main():
    args = parse_args()

    sizes = SIZES if args.size == "all" else [args.size]
    results = []

    print("Starting benchmark...\n")
    print(f"Region: {args.region} | Dates: {args.date_start} → {args.date_end} | Runs: {args.runs}\n")

    for size in sizes:
        for layout in LAYOUTS:
            path = os.path.join(BASE_DIR, size, layout)
            s3_prefix = f"curated/ubereats/{size}/{layout}/"

            print(f"=== {size} - {layout} ===")

            if not os.path.exists(path):
                print("Path missing, skipping\n")
                continue

            # 1. Listing local : nombre de fichiers et taille totale
            listing_time, file_count, total_bytes = count_files_and_bytes(path)

            # 2. Listing S3 : même mesure mais côté bucket MinIO
            try:
                s3_listing_time, s3_object_count, s3_total_bytes = list_s3_objects(s3_prefix)
            except Exception:
                s3_listing_time, s3_object_count, s3_total_bytes = None, None, None

            # 3. Upload/download throughput sur un fichier échantillon du layout
            sample_file = None
            for root, dirs, files in os.walk(path):
                for f in files:
                    if f.endswith(".parquet"):
                        sample_file = os.path.join(root, f)
                        break
                if sample_file:
                    break

            upload_throughput = None
            download_throughput = None

            if sample_file:
                s3_key = f"bench_tmp/{size}_{layout}_sample.parquet"
                try:
                    upload_throughput = measure_upload_throughput(sample_file, s3_key)
                    download_throughput = measure_download_throughput(s3_key, "/tmp/bench_download.parquet")
                except Exception:
                    pass

            # 4. Selective Query : médiane sur runs répétitions
            sel_times = []
            sel_rows = None
            for _ in range(args.runs):
                t, rows, _ = run_selective_query(path, args.region, args.date_start, args.date_end, layout)
                sel_times.append(t)
                sel_rows = rows
            sel_time = median(sel_times)

            # 5. Broad Query : médiane sur runs répétitions
            broad_times = []
            broad_rows = None
            for _ in range(args.runs):
                t, rows, _ = run_broad_query(path)
                broad_times.append(t)
                broad_rows = rows
            broad_time = median(broad_times)

            # Affichage des résultats
            print(f"Local listing:      {listing_time:.4f}s | files: {file_count}")
            print(f"S3 listing:         {s3_listing_time if s3_listing_time else 'N/A'}s | objects: {s3_object_count if s3_object_count else 'N/A'}")
            print(f"Upload throughput:  {upload_throughput if upload_throughput else 'N/A'} MB/s")
            print(f"Download throughput:{download_throughput if download_throughput else 'N/A'} MB/s")
            print(f"Selective query:    {sel_time:.4f}s | rows: {sel_rows}")
            print(f"Broad query:        {broad_time:.4f}s | rows: {broad_rows}")
            print()

            # Stockage des résultats pour le CSV
            results.append({
                "size": size,
                "layout": layout,
                "local_listing_time_s": round(listing_time, 4),
                "local_file_count": file_count,
                "local_total_size_gb": round(total_bytes / 1e9, 4),
                "s3_listing_time_s": round(s3_listing_time, 4) if s3_listing_time else None,
                "s3_object_count": s3_object_count,
                "s3_total_size_gb": round(s3_total_bytes / 1e9, 4) if s3_total_bytes else None,
                "upload_throughput_mbs": round(upload_throughput, 2) if upload_throughput else None,
                "download_throughput_mbs": round(download_throughput, 2) if download_throughput else None,
                "selective_time_s": round(sel_time, 4),
                "selective_rows": sel_rows,
                "broad_time_s": round(broad_time, 4),
                "broad_rows": broad_rows,
            })

    if not results:
        print("No results to save.")
        return
    
    # Sauvegarde de tous les résultats dans results.csv
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"Results saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
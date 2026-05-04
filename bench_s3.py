import csv
import time
import boto3


# ===== CONFIG =====

BUCKET = "ccbd"
RESULTS_FILE = "results_s3.csv"

SIZES = ["S", "M", "L"]
LAYOUTS = ["flat", "by_date", "by_region"]

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)


# ===== S3 LISTING =====

def list_s3_objects(prefix):
    # mesure le temps de listing S3 pour un prefix donné
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


# ===== MAIN BENCH =====

def main():
    results = []

    print("Starting S3 benchmark...\n")

    for size in SIZES:
        for layout in LAYOUTS:
            prefix = f"curated/ubereats/{size}/{layout}/"

            print(f"=== {size} - {layout} ===")

            listing_time, object_count, total_bytes = list_s3_objects(prefix)

            print(f"S3 listing time: {listing_time:.4f}s")
            print(f"Object count: {object_count}")
            print(f"Total size GB: {total_bytes / 1e9:.2f}")
            print()

            results.append({
                "size": size,
                "layout": layout,
                "s3_prefix": prefix,
                "s3_listing_time_s": listing_time,
                "object_count": object_count,
                "total_size_gb": total_bytes / 1e9,
            })

    # écrire les résultats dans un CSV
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"Results saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
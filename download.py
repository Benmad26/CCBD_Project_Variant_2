import os
import boto3

# ===== CONFIG =====

# MinIO bucket name
BUCKET = "ccbd"

# Connection to MinIO (local S3)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)


# ===== DOWNLOAD =====
# Downloads all files from an S3 prefix to a local folder
# Uses a paginator to handle large numbers of objects (>1000)
# Recreates the folder structure locally
# - s3_prefix : path in the bucket (ex: curated/ubereats/S/flat)
# - local_dir : local destination folder (ex: data/curated/S/flat)

def download_directory(s3_prefix, local_dir):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]

            # relative path from the S3 prefix
            # ex: curated/ubereats/S/flat/part0.parquet → part0.parquet
            relative_path = s3_key[len(s3_prefix):].lstrip("/")
            local_path = os.path.join(local_dir, relative_path)

            # create subfolders if needed (ex: date=2026-01-10/)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # download the file
            s3.download_file(BUCKET, s3_key, local_path)
            print(f"{s3_key} → {local_path}")


# ===== MAIN =====
# Downloads the 3 layouts (flat, by_date, by_region) for all sizes (S, M, L)
# S3 source structure:
#   curated/ubereats/S/flat/...
#   curated/ubereats/S/by_date/...
#   curated/ubereats/S/by_region/...
# Local destination structure:
#   data/curated/S/flat/...
#   data/curated/S/by_date/...
#   data/curated/S/by_region/...

def main():
    for size in ["S", "M", "L"]:
        for layout in ["flat", "by_date", "by_region"]:
            s3_prefix = f"curated/ubereats/{size}/{layout}"
            local_dir = f"data_down/curated/{size}/{layout}"

            print(f"\nDownloading curated/{size}/{layout}...")
            download_directory(s3_prefix, local_dir)

    print("\nDownload complete!")


if __name__ == "__main__":
    main()
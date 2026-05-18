import os
import boto3

# bucket MinIO
BUCKET = "ccbd"

# Connection to MinIO (local S3-compatible storage)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

# UPLOAD

# Uploads all files from a local folder to S3
# Recreates the folder structure in the bucket
# - local_dir: local source folder
# - s3_prefix: destination path in the bucket
def upload_directory(local_dir, s3_prefix):
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            relative_path = relative_path.replace("\\", "/")
            s3_path = f"{s3_prefix}/{relative_path}"
            s3.upload_file(local_path, BUCKET, s3_path)
            print(f"{local_path} → {s3_path}")


# MAIN
# Uploads the 3 layouts (flat, by_date, by_region) for all sizes (S, M, L)
# to curated/ubereats/<size>/<layout>/ in the bucket
def main():
    for size in ["S", "M", "L"]:
        for layout in ["flat", "by_date", "by_region"]:
            local_dir = f"data/curated/{size}/{layout}"
            s3_prefix = f"curated/ubereats/{size}/{layout}"

            if not os.path.exists(local_dir):
                print(f"Missing folder, skipping : {local_dir}")
                continue

            print(f"\nUploading curated/{size}/{layout}...")
            upload_directory(local_dir, s3_prefix)

    print("\nUpload complete!")


if __name__ == "__main__":
    main()

import os
import boto3

# bucket MinIO
BUCKET = "ccbd"

# connexion à MinIO (S3 local)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

def upload_directory(local_dir, s3_prefix):
    """
    Upload tous les fichiers d'un dossier vers S3.
    - local_dir : dossier local
    - s3_prefix : chemin dans le bucket
    """
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            relative_path = relative_path.replace("\\", "/")
            s3_path = f"{s3_prefix}/{relative_path}"
            s3.upload_file(local_path, BUCKET, s3_path)
            print(f"{local_path} → {s3_path}")


def main():
    """
    Upload curated (flat, by_date, by_region) pour S/M/L
    Structure S3 :
      curated/ubereats/S/flat/...
      curated/ubereats/S/by_date/...
      curated/ubereats/S/by_region/...
      ...
    """
    for size in ["S", "M", "L"]:
        for layout in ["flat", "by_date", "by_region"]:
            local_dir = f"data/curated/{size}/{layout}"
            s3_prefix = f"curated/ubereats/{size}/{layout}"

            if not os.path.exists(local_dir):
                print(f"Dossier manquant, ignoré : {local_dir}")
                continue

            print(f"\nUploading curated/{size}/{layout}...")
            upload_directory(local_dir, s3_prefix)

    print("\nUpload terminé !")


if __name__ == "__main__":
    main()

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
    Upload tous les fichiers .parquet d’un dossier vers S3.

    - local_dir : dossier local
    - s3_prefix : chemin dans le bucket
    """

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)

                # chemin relatif (garde structure dossiers)
                relative_path = os.path.relpath(local_path, local_dir)

                # IMPORTANT (Windows fix)
                # remplace "\" par "/" pour S3
                relative_path = relative_path.replace("\\", "/")

                # chemin final dans S3
                s3_path = f"{s3_prefix}/{relative_path}"

                # upload fichier
                s3.upload_file(local_path, BUCKET, s3_path)

                print(f"{local_path} → {s3_path}")


def main():
    """
    Upload toutes les tailles S / M / L
    dans le bucket MinIO
    """

    for size in ["S", "M", "L"]:
        local_dir = f"data/curated/{size}"
        s3_prefix = f"curated/ubereats/{size}"

        print(f"\nUploading {size}...")
        upload_directory(local_dir, s3_prefix)

    print("\nUpload terminé !")


if __name__ == "__main__":
    main()
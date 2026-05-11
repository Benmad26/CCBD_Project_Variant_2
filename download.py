import os
import boto3

# ===== CONFIG =====

# Nom du bucket MinIO
BUCKET = "ccbd"

# Connexion à MinIO (S3 local)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)


# ===== DOWNLOAD =====
# Télécharge tous les fichiers d'un préfixe S3 vers un dossier local
# Utilise un paginator pour gérer les grands nombres d'objets (>1000)
# Recrée la structure de dossiers localement
# - s3_prefix : chemin dans le bucket (ex: curated/ubereats/S/flat)
# - local_dir : dossier local de destination (ex: data2/curated/S/flat)

def download_directory(s3_prefix, local_dir):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]

            # chemin relatif par rapport au préfixe S3
            # ex: curated/ubereats/S/flat/part0.parquet → part0.parquet
            relative_path = s3_key[len(s3_prefix):].lstrip("/")
            local_path = os.path.join(local_dir, relative_path)

            # crée les sous-dossiers si nécessaire (ex: date=2026-01-10/)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # téléchargement du fichier
            s3.download_file(BUCKET, s3_key, local_path)
            print(f"{s3_key} → {local_path}")


# ===== MAIN =====
# Télécharge les 3 layouts (flat, by_date, by_region) pour les 3 tailles (S, M, L)
# Structure S3 source :
#   curated/ubereats/S/flat/...
#   curated/ubereats/S/by_date/...
#   curated/ubereats/S/by_region/...
#   ...
# Structure locale destination :
#   data2/curated/S/flat/...
#   data2/curated/S/by_date/...
#   data2/curated/S/by_region/...
#   ...

def main():
    for size in ["S", "M", "L"]:
        for layout in ["flat", "by_date", "by_region"]:
            s3_prefix = f"curated/ubereats/{size}/{layout}"
            local_dir = f"data_down/curated/{size}/{layout}"

            print(f"\nDownloading curated/{size}/{layout}...")
            download_directory(s3_prefix, local_dir)

    print("\nDownload terminé !")


if __name__ == "__main__":
    main()
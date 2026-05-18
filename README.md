# CCBD SP26 – Variant 2: Partitioning Strategies

**Cloud Computing and Big Data – 25/26**  
**Institut d'Informatique – Université de Neuchâtel**  
**Team:** Bernardo Simao, Paul Micheli

---

## Overview

This project benchmarks three Parquet partitioning layouts for a synthetic food-delivery event dataset stored on S3-compatible object storage (MinIO). The three layouts are:

- **flat** – single Parquet file, no partitioning
- **by_date** – Hive partitioning on `date=YYYY-MM-DD`
- **by_region** – Hive partitioning on `region=<city>`

Metrics measured: S3 listing time, upload/download throughput, selective query time, broad query time.

---

## Dependencies

| Tool | Version tested | Install |
|---|---|---|
| Python | 3.10+ | — |
| pyarrow | 14+ | `pip install pyarrow` |
| boto3 | 1.34+ | `pip install boto3` |
| numpy | 1.26+ | `pip install numpy` |
| pandas | 2.0+ | `pip install pandas` |
| matplotlib | 3.8+ | `pip install matplotlib` |
| MinIO | latest | https://min.io/download |

Install all Python dependencies at once:

```bash
pip install pyarrow boto3 numpy pandas matplotlib
```

---

### Step 3 – Upload to MinIO
 
```bash
python upload.py
```
 
Reads layouts from `data2/curated/<size>/<layout>/` and uploads them to the bucket under `curated/ubereats/<size>/<layout>/`.
 
> **Note:** `upload.py` reads from `data2/curated/` (output of `make_layouts.py` when using `--output-dir data2`). Make sure Step 2 used the same output directory.

Once running, create the `ccbd` bucket via the MinIO console at http://localhost:9001 (login: minioadmin / minioadmin).

**Endpoint configuration used in all scripts:**

| Parameter | Value |
|---|---|
| Endpoint URL | http://localhost:9000 |
| Access key | minioadmin |
| Secret key | minioadmin |
| Bucket | ccbd |

---

## How to Reproduce Results

Run the following steps in order. All commands are run from the repository root.

### Step 1 – Generate datasets

```bash
python dataset_gen.py --size S --output-dir data
python dataset_gen.py --size M --output-dir data
python dataset_gen.py --size L --output-dir data
```

This produces `data/dataset_S.parquet`, `data/dataset_M.parquet`, `data/dataset_L.parquet`.

Default seed: `42`. To use a different seed: `--seed <N>`.

### Step 2 – Create partitioned layouts

```bash
python make_layouts.py
```

This reads each raw Parquet file from `data/` and writes three layouts under `data/curated/<size>/<layout>/`:

```
data/curated/
  S/
    flat/data.parquet
    by_date/date=2026-01-01/...
    by_region/region=Zurich/...
  M/ ...
  L/ ...
```

Optional: control batch size with `--batch-size 500000`.

### Step 3 – Upload to MinIO

```bash
python upload.py
```

Reads layouts from `data2/curated/<size>/<layout>/` and uploads them to the bucket under `curated/ubereats/<size>/<layout>/`.

> **Note:** `upload.py` reads from `data2/curated/` (output of `make_layouts.py` when using `--output-dir data2`). Make sure Step 2 used the same output directory.

### Step 4 – Run the benchmark

```bash
python bench.py --size all --region Zurich --date-start 2026-01-10 --date-end 2026-01-12 --runs 3
```

This runs the full benchmark suite for all sizes and layouts and writes `results.csv`.

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `--size` | `all` | One of `S`, `M`, `L`, or `all` |
| `--region` | `Zurich` | Region used for the selective query filter |
| `--date-start` | `2026-01-10` | Start date for selective query (YYYY-MM-DD) |
| `--date-end` | `2026-01-12` | End date for selective query (YYYY-MM-DD) |
| `--runs` | `3` | Number of repetitions (median is reported) |

### Step 5 – Analyse results

Open and run the notebook:

```bash
jupyter notebook analysis.ipynb
```

The notebook loads `results.csv` and produces all plots and tables. It must be run after Step 4.

### Step 6 – (Optional) Download from MinIO

```bash
python download.py
```

Downloads all layouts from MinIO to `data_down/curated/<size>/<layout>/`.

---

## Repository Structure

```
.
├── dataset_gen.py      # Synthetic dataset generator
├── make_layouts.py     # Creates flat / by_date / by_region layouts
├── upload.py           # Uploads curated layouts to MinIO (S3)
├── download.py         # Downloads layouts from MinIO
├── bench.py            # Benchmark script → results.csv
├── analysis.ipynb      # Analysis notebook (plots + tables)
├── results.csv         # Pre-computed benchmark results
├── README.md           # This file
```



## Notes

- All timing measurements use `time.time()` wall-clock time.
- Query times are the **median** over `--runs` repetitions to reduce OS cache and scheduling noise.
- The flat layout stores one large file; by_date and by_region use Hive-style directory partitioning readable by pyarrow with `partitioning="hive"`.
- MinIO is used as a drop-in S3 replacement; the same code works with AWS S3 by changing the `endpoint_url` and credentials.

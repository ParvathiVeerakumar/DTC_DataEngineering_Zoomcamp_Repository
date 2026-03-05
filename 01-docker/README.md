# docker-workshop

A small workshop repository that demonstrates ingesting NYC taxi data into PostgreSQL using Docker, Python and simple CLI tools.

## What this contains

- `Dockerfile` and `docker-compose.yaml` for running PostgreSQL and pgAdmin.
- `pipeline/ingest_data.py` — CLI to ingest CSV (yellow taxi 2021) files into Postgres using pandas.
- `pipeline/ingest_data_2025.py` — CLI to ingest Parquet (green taxi 2025) files into Postgres (reads full parquet and uploads in chunks).
- `pipeline/` contains helper scripts and SQL used in the workshop.

## Prerequisites

- Docker and Docker Compose (v2) installed and running.
- Python 3.11+ for local script runs (project targets Python 3.13 in `pyproject.toml`; adjust as needed).
- On Linux/WSL, installing system `libpq` is required if you install `psycopg` from source (see notes).

## Quickstart — Docker Compose

From the repository root:



Service defaults (see `docker-compose.yaml`):
- Postgres: host `localhost`, port `5432`, database `ny_taxi`, user `root`, password `root`.
- pgAdmin: http://localhost:8085, login `admin@admin.com` / `root`.

Note: If port 5432 is already in use, either stop the conflicting service or change the host port mapping in `docker-compose.yaml`.

## Running the ingestion scripts locally

Create and activate a virtual environment, then install runtime dependencies:

Windows (PowerShell):


Use pgAdmin at http://localhost:8085 (credentials above) to browse databases and run SQL.

If you prefer interactive psql-like experience for convenience, `pgcli` is useful — install it in your environment or in the `dev` group.

## Notes and troubleshooting

- psycopg and libpq: Installing `psycopg[binary]` will use a prebuilt binary wheel when available. If pip attempts to build from source you'll need system `libpq` and build tools (Debian/Ubuntu: `libpq-dev build-essential pkg-config`).

- Parquet streaming: pandas' `read_parquet` does not support `iterator`/`chunksize`. `ingest_data_2025.py` reads the parquet file and uploads in row chunks. For very large parquet files use a PyArrow dataset scanner to stream batches (requires `pyarrow` and potentially `fsspec` for remote HTTP access).

- Docker data path: the Postgres data volume is mapped in `docker-compose.yaml`. The official Postgres directory is typically `/var/lib/postgresql/data`; adjust the compose volume if you rely on that path.

#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> None:
    """
    Read a parquet file (local or remote) and write it to Postgres in row-chunks.

    Notes:
    - pandas.read_parquet does not support iterator/chunksize. We read the file
      into a DataFrame and upload it in slices to avoid passing unsupported
      arguments to pandas.
    - If the parquet is large and memory is a concern, replace this implementation
      with a pyarrow.dataset scanner to stream batches instead.
    """
    # Read parquet (let pandas choose pyarrow/fastparquet backend)
    df = pd.read_parquet(url)

    # Ensure date columns are proper datetimes if present
    for col in parse_dates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Optionally coerce dtypes if columns exist (parquet often preserves types)
    for col, typ in dtype.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(typ)
            except Exception:
                # If conversion fails, continue — parquet typically preserves numeric types
                pass

    if df.empty:
        print("No rows to ingest.")
        return

    # Create table schema (empty) and insert in chunks
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"Table {target_table} created")

    n_rows = len(df)
    inserted = 0

    for start in tqdm(range(0, n_rows, chunksize), desc="ingesting"):
        end = min(start + chunksize, n_rows)
        chunk = df.iloc[start:end]
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
        )
        inserted += len(chunk)
        print(f"Inserted chunk rows {start}:{end} ({len(chunk)})")

    print(f"done ingesting {inserted} rows to {target_table}")


@click.command()
@click.option("--pg-user", default="postgres", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="postgres", show_default=True, help="Postgres password")
@click.option("--pg-host", default="db", show_default=True, help="Postgres host")
@click.option("--pg-port", default="5432", show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--year", default=2025, show_default=True, type=int, help="Data year")
@click.option("--month", default=11, show_default=True, type=int, help="Data month (1-12)")
@click.option("--chunksize", default=100000, show_default=True, type=int, help="Row chunk size for DB inserts")
@click.option("--target-table", default="green_taxi_data_2025", show_default=True, help="Target table name")
def main(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    year: int,
    month: int,
    chunksize: int,
    target_table: str,
) -> None:
    """Ingest NYC green taxi parquet data into a Postgres table."""
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f'{url_prefix}/green_tripdata_{year:04d}-{month:02d}.parquet'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )


if __name__ == '__main__':
    main()
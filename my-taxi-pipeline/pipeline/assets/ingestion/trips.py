"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
    description: A code indicating the TPEP provider that provided the record
  - name: pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles reported by the taximeter
  - name: pickup_longitude
    type: float
    description: Longitude where the meter was engaged
  - name: pickup_latitude
    type: float
    description: Latitude where the meter was engaged
  - name: rate_code_id
    type: integer
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
  - name: dropoff_longitude
    type: float
    description: Longitude where the meter was disengaged
  - name: dropoff_latitude
    type: float
    description: Latitude where the meter was disengaged
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid for the trip
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax that is automatically triggered based on the metered rate in use
  - name: tip_amount
    type: float
    description: Tip amount - This field is automatically populated for credit card tips
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed trips at the flag drop
  - name: total_amount
    type: float
    description: The total amount charged to passengers
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge for trips that start, end or pass through the congestion zone
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the data was extracted

@bruin"""

import os
import pandas as pd
import requests
from datetime import datetime
from dateutil import parser as date_parser
import json

def materialize():
    """
    Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Get environment variables
    start_date = os.getenv('BRUIN_START_DATE')
    end_date = os.getenv('BRUIN_END_DATE')
    bruin_vars = json.loads(os.getenv('BRUIN_VARS', '{}'))
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])

    print(f"Fetching taxi data from {start_date} to {end_date} for types: {taxi_types}")

    # Base URL for NYC taxi data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    # Generate list of months to fetch
    start = date_parser.parse(start_date)
    end = date_parser.parse(end_date)

    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    all_dataframes = []

    for taxi_type in taxi_types:
        for month in months:
            filename = f"{taxi_type}_tripdata_{month}.parquet"
            url = base_url + filename

            try:
                print(f"Fetching {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Read parquet data
                df = pd.read_parquet(pd.io.common.BytesIO(response.content))

                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()

                all_dataframes.append(df)
                print(f"Successfully loaded {len(df)} rows for {taxi_type} {month}")

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch {url}: {e}")
                continue
            except Exception as e:
                print(f"Error processing {url}: {e}")
                continue

    if not all_dataframes:
        raise ValueError("No data was successfully fetched")

    # Concatenate all dataframes
    final_df = pd.concat(all_dataframes, ignore_index=True)

    print(f"Total rows fetched: {len(final_df)}")
    return final_df



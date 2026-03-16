/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: vendor_id
    type: integer
    description: A code indicating the TPEP provider that provided the record
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles reported by the taximeter
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid for the trip
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: The total amount charged to passengers
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type name from lookup
  - name: trip_duration_minutes
    type: integer
    description: Trip duration in minutes
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: valid_payment_types
    description: Ensure all payment types exist in lookup table
    query: |
      SELECT COUNT(*) FROM staging.trips
      WHERE payment_type_name IS NULL
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH deduplicated_trips AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        passenger_count,
        trip_distance,
        fare_amount
      ORDER BY extracted_at DESC
    ) as rn
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    -- Basic data quality filters
    AND vendor_id IS NOT NULL
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
)

SELECT
  dt.vendor_id,
  dt.pickup_datetime,
  dt.dropoff_datetime,
  dt.passenger_count,
  dt.trip_distance,
  dt.pickup_longitude,
  dt.pickup_latitude,
  dt.rate_code_id,
  dt.store_and_fwd_flag,
  dt.dropoff_longitude,
  dt.dropoff_latitude,
  dt.payment_type,
  dt.fare_amount,
  dt.extra,
  dt.mta_tax,
  dt.tip_amount,
  dt.tolls_amount,
  dt.improvement_surcharge,
  dt.total_amount,
  dt.congestion_surcharge,
  dt.taxi_type,
  pl.payment_type_name,
  CAST((EPOCH(dt.dropoff_datetime) - EPOCH(dt.pickup_datetime)) / 60 AS INTEGER) as trip_duration_minutes
FROM deduplicated_trips dt
LEFT JOIN ingestion.payment_lookup pl
  ON dt.payment_type = pl.payment_type_id
WHERE dt.rn = 1

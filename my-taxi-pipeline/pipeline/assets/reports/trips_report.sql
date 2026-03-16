/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: pickup_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: pickup_date
    type: date
    description: Date of trip pickup
    primary_key: true
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment method used
    primary_key: true
  - name: trip_count
    type: integer
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: Total fare amount collected
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: float
    description: Total tip amount collected
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: Average trip distance
    checks:
      - name: non_negative
  - name: avg_trip_duration_minutes
    type: float
    description: Average trip duration in minutes
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  CAST(pickup_datetime AS DATE) as pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) as trip_count,
  SUM(fare_amount) as total_fare_amount,
  SUM(tip_amount) as total_tip_amount,
  AVG(trip_distance) as avg_trip_distance,
  AVG(trip_duration_minutes) as avg_trip_duration_minutes
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name

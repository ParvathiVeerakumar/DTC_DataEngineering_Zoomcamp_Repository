"""@bruin
name: ingestion.trips
type: python
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import sys

# Set timezone handling before any other imports
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# Try to locate tzdata and set TZPATH if found
try:
    import tzdata
    tzdata_path = os.path.dirname(tzdata.__file__)
    os.environ["TZPATH"] = tzdata_path
except ImportError:
    pass

import json
import pandas as pd
import requests # type: ignore
from datetime import datetime, timedelta
from io import BytesIO


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Fetches parquet files for the specified date range and taxi types,
    concatenates them, and returns the data with pickup and dropoff datetime columns.
    """
    # Get environment variables
    start_date_str = os.environ.get('BRUIN_START_DATE', '2022-01-01')
    end_date_str = os.environ.get('BRUIN_END_DATE', '2022-01-31')
    bruin_vars = json.loads(os.environ.get('BRUIN_VARS', '{}'))
    
    # Parse dates - handle both YYYY-MM-DD and ISO formats
    try:
        # Try ISO format first (with T and Z)
        if 'T' in start_date_str:
            start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
        else:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            
        if 'T' in end_date_str:
            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
        else:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Could not parse dates: start={start_date_str}, end={end_date_str}. Error: {e}")
    
    # Get taxi types from pipeline variable
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])
    
    # TLC data endpoint
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    
    # Generate list of months to fetch (based on pickup_datetime in the data)
    # Note: NYC taxi data is organized by month, so we fetch monthly files
    # that could contain trips within our date range
    months_to_fetch = set()
    current = start_date.replace(day=1)  # Start of month
    while current <= end_date:
        months_to_fetch.add((current.year, current.month))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)
    
    # Fetch data for each taxi type and month
    dfs = []
    
    for taxi_type in taxi_types:
        for year, month in sorted(months_to_fetch):
            filename = f'{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
            url = base_url + filename
            
            try:
                print(f'Fetching {url}...')
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Read parquet file
                df = pd.read_parquet(BytesIO(response.content))
                
                # Rename columns to standardized names (handle both yellow and green taxis)
                column_mapping = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                }
                df = df.rename(columns=column_mapping)
                
                # Filter to only the required columns
                required_columns = ['pickup_datetime', 'dropoff_datetime']
                df = df[required_columns]
                
                # Convert datetime columns to timezone-naive to avoid PyArrow timezone issues
                for col in required_columns:
                    if col in df.columns:
                        # Convert to datetime if not already, then strip timezone
                        dt_col = pd.to_datetime(df[col])
                        if dt_col.dt.tz is not None:
                            # Has timezone - convert to UTC first, then remove timezone
                            dt_col = dt_col.dt.tz_convert('UTC').dt.tz_localize(None)
                        df[col] = dt_col
                
                dfs.append(df)
                print(f'Successfully fetched {filename} ({len(df)} rows)')
                
            except requests.exceptions.RequestException as e:
                print(f'Warning: Could not fetch {filename}: {e}')
                continue
            except Exception as e:
                print(f'Warning: Error processing {filename}: {e}')
                continue
    
    if not dfs:
        print(f'No data fetched for taxi types {taxi_types} and date range {start_date_str} to {end_date_str}')
        print('This might be expected if the date range is in the future or data is unavailable.')
        # Return empty dataframe with correct schema instead of raising error
        empty_df = pd.DataFrame(columns=['pickup_datetime', 'dropoff_datetime'])
        return empty_df
    
    # Concatenate all dataframes
    result = pd.concat(dfs, ignore_index=True)
    
    print(f'Total rows fetched: {len(result)}')
    return result
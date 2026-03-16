import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
df = spark.read.option("header", "true").parquet("code/yellow_tripdata_2025-11.parquet")
df.show()
df = df.repartition(4)
import os

output_path = "code/fhvhv/2025/11"
os.makedirs(output_path, exist_ok=True)

df.write.mode("overwrite").parquet(output_path)

zone_df = spark.read.option("header", "true").csv("data/taxi_zone_lookup.csv")
pickup_counts = df.groupBy("PULocationID") \
    .agg(F.count("*").alias("trip_count"))

result = pickup_counts.join(
    zone_df,
    pickup_counts.PULocationID == zone_df.LocationID
)

print("Top 5 zones by trip count:")
result.select("Zone", "trip_count") \
    .orderBy("trip_count") \
    .show(5)

import os

parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
sizes = [os.path.getsize(os.path.join(output_path, f)) for f in parquet_files]
avg_size_mb = sum(sizes) / len(sizes) / (1024*1024)
print(f"Average Parquet file size: {avg_size_mb:.2f} MB") 


# Filter for November 15 (any year, here assuming 2025)
df_15_nov = df.filter(F.to_date(df.tpep_pickup_datetime) == F.lit("2025-11-15"))

# Count trips
num_trips = df_15_nov.count()
print(f"Number of trips on Nov 15: {num_trips}")

# Make sure columns are timestamp
df = df.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime")) \
       .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))

# Calculate trip duration in hours
df = df.withColumn("trip_hours", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 3600)

# Find the longest trip
longest_trip = df.agg(F.max("trip_hours")).collect()[0][0]
print(f"Longest trip duration (hours): {longest_trip:.1f}")

spark.stop()

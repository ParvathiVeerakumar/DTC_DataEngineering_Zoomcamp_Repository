import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('taxi_zones') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

#df = spark.range(10)
df = spark.read \
    .option("header", "true") \
    .csv("data/taxi_zone_lookup.csv")
df.show()

df.write.mode("overwrite").parquet("data/output_parquet")
spark.stop()
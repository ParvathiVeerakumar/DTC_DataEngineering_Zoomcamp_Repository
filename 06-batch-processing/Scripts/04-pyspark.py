import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
print(f"Pandas version: {pd.__version__}")

df_pandas = pd.read_csv('Scripts/head.csv')
print("Pandas DataFrame:")
print(df_pandas)

print("dtypes of pandas dataframe: ")
print(df_pandas.dtypes)

print("Schema of Spark DataFrame:")
print(spark.createDataFrame(df_pandas).schema)

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('code/fhvhv_tripdata_2021-01.csv.gz')
df = df.repartition(24)
import os

output_path = "code/fhvhv/2021/01"
os.makedirs(output_path, exist_ok=True)

df.write.mode("overwrite").parquet(output_path)


import os

parquet_files = [f for f in os.listdir('code/fhvhv/2021/01/') if f.endswith(".parquet")]
sizes = [os.path.getsize(os.path.join('code/fhvhv/2021/01/', f)) for f in parquet_files]
print("Sizes of Parquet files (in bytes):")
for f, size in zip(parquet_files, sizes):
    print(f"{f}: {size} bytes")
avg_size_mb = sum(sizes) / len(sizes) / (1024*1024)
print(f"Average Parquet file size: {avg_size_mb:.2f} MB") 

df = spark.read.parquet('code/fhvhv/2021/01/')
print("Schema of Spark DataFrame after reading parquet:")
df.printSchema()
df.show()

def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'
print(f"crazy_stuff('B02884') = {crazy_stuff('B02884')}")

crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
print("Base ID transformations:")
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()

print("Filtering for HV0003 license number:")
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
  .filter(df.hvfhs_license_num == 'HV0003')

spark.stop()
# docker compose up -d
#docker exec -it spark ls scripts
# docker exec -it spark /opt/spark/bin/spark-submit scripts/04-pyspark.py
#& c:\Users\User\source\repos\DTC_DataEngineering_Zoomcamp_Repository\.venv\Scripts\Activate.ps1

#docker compose down
#docker compose pull

# to install pandas in docker as user:
#docker exec -it spark bash
#pip install --user pandas

# to install pandas in docker as root:
#docker exec -it --user root spark /bin/bash
#pip install pandas

# Build a custom Docker image

#If you want to run Spark in Docker with pandas always available, create a custom Docker image:
# FROM bitnami/spark:3.3.2

#USER root

#RUN pip install pandas

#USER 1001
#Build a custom Docker image

#If you want to run Spark in Docker with pandas always available, create a custom Docker image:
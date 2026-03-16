import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
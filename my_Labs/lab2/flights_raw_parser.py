from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


#instantiate or ge an existing Spark session using the following code 
spark = SparkSession.builder.master("local").appName('ex2_flights').getOrCreate()

#read the raw data from S3/.../*.csv
flights_raw_df = spark.read.csv('s3a://spark/data/raw/flights_raw/',header = True)

#create dataset from raw data wiht transfomation
flight_df = flights_raw_df.select(
    F.col('DayofMonth').cast(T.IntegerType()).alias('day_of_month'),
    F.col('DayOfWeek').cast(T.IntegerType()).alias('day_of_week'),
    F.col('Carrier').alias('carrier'),
    F.col('OriginAirportID').cast(T.IntegerType()).alias('origin_airport_id'),
    F.col('DestAirportID').cast(T.IntegerType()).alias('dest_airport_id'),
    F.col('DepDelay').cast(T.IntegerType()).alias('dep_delay'),
    F.col('ArrDelay').cast(T.IntegerType()).alias('arr_delay')
)

#write df in parquet format 
flight_df.write.parquet('s3a://spark/data/source/flights_raw/', mode='overwrite')
spark.stop()
# Importing necessary modules for the exercise
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


spark = SparkSession\
    .builder\
    .master("local")\
    .config("spark.driver.memory","4g")\
    .appName("ex3_clean_flights")\
    .getOrCreate()

flights_df = spark.read.parquet('s3a://spark/data/source/flights/')
flights_raw_df = spark.read.parquet('s3a://spark/data/source/flights_raw/')

#removes duplicates
flights_distinct_df = flights_df.dropDuplicates()
flights_raw_distinct_df =flights_raw_df.dropDuplicates()

#Mached_data

flights_matched_df = flights_distinct_df.intersect(flights_raw_distinct_df)


#UnMatched_data
unmatched_flights = flights_distinct_df\
                        .subtract(flights_matched_df)\
                        .withColumn('source_of_data',F.lit('flights'))

                                    
unmatched_flights_raw = flights_raw_distinct_df\
                        .subtract(flights_matched_df)\
                        .withColumn('source_of_data',F.lit('flights_raw'))

unmatched_flights_df = unmatched_flights.union(unmatched_flights_raw)

#save the news files

flights_matched_df.write.parquet('s3a://spark/data/stg/flight_matched/',mode='overwrite')
unmatched_flights_df.write.parquet('s3a://spark/data/stg/flight_unmatched/',mode='overwrite')


spark.stop()
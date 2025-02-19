from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession\
    .builder\
    .master('local[*]')\
    .appName('ex4_anomalies_detection')\
    .config('spark.driver.memory','4g')\
    .getOrCreate()

sliding_range_window = Window\
        .partitionBy(F.col('carrier'))\
        .orderBy(F.col('start_range'))

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
flights_df.cache()

grouped_df = flights_df \
 .groupBy(F.col('Carrier'), F.window(F.col('flight_date'), '10 days', '1 day').alias('date_window')) \
 .agg(F.sum(F.col('dep_delay') + F.col('arr_delay')).alias('total_delay'))




structured_df = grouped_df \
        .select(F.col('Carrier'),
        F.col('date_window.start').alias('start_range'),
        F.col('date_window.end').alias('end_range'),
        F.col('total_delay'))
 

change_df = structured_df \
 .withColumn('last_window_delay', F.lag(F.col('total_delay')).over(sliding_range_window)) \
 .withColumn('change_percent', F.abs(F.lit(1.0) - (F.col('total_delay') /
F.col('last_window_delay'))))


significant_changes_df = change_df.where(F.col('change_percent') > F.lit(0.3))
significant_changes_df.show(100)



flights_df.unpersist()
spark.stop() 
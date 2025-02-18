from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row


def get_dates_df():
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    in_dates_df = dummy_df\
                    .select(
                        F.explode(
                            F.sequence(
                                F.lit("2020-01-01").cast(T.DateType()), 
                                F.lit("2020-12-31").cast(T.DateType())
                                )
                            ).alias("flight_date"))
    return in_dates_df
 

spark = SparkSession\
        .builder\
        .master("local")\
        .appName("add_missing_dates")\
        .getOrCreate()


flight_matched_df = spark.read.parquet('s3a://spark/data/stg/flight_matched/')
dates_df = get_dates_df()

dates_full_df = dates_df\
                .withColumn('day_of_week',F.dayofweek(F.col('flight_date')))\
                .withColumn('day_of_month',F.dayofmonth(F.col('flight_date')))





max_date_df = dates_full_df\
                .groupby(F.col('day_of_week'),F.col('day_of_month'))\
                .agg(F.max(F.col('flight_date')).alias('flight_date'))



enriched_flights_df = flight_matched_df.join(max_date_df, ['day_of_week', 'day_of_month'])
enriched_flights_df.write.parquet('s3a://spark/data/transformed/flights/',
mode='overwrite')
 


spark.stop()
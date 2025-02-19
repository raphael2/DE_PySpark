from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession\
        .builder\
        .master('local[*]')\
        .config('spark.driver.memory','4g')\
        .getOrCreate()

unbounded_window = Window\
        .partitionBy(F.col('carrier'))\
        .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
flights_df.cache()


avg_delay_df = flights_df\
                .withColumn(
                    'avg_all_time',
                    F.avg('arr_delay').over(unbounded_window)
                )

avg_diff_percent_df = avg_delay_df\
                    .withColumn('avg_diff_percent',
                    F.abs(F.col('arr_delay')/F.col('avg_all_time'))
                    )

avg_diff_percent_df.where(F.col('avg_diff_percent')>F.lit(5.0)).show()

flights_df.unpersist()
spark.stop()
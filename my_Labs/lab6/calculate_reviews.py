from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName('ex6_calculate_reviews') \
        .getOrCreate()
        #.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        #.getOrCreate()


static_data_df = spark.read.parquet('s3a://spark/data/source/google_apps/')
static_data_df.cache()

static_data_df.show()




static_data_df.unpersist()
spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession\
        .builder\
        .master('local')\
        .config('spark.driver.memory', '4g')\
        .appName('ex4_anomalies_detection')\
        .getOrCreate()

sliding_range_window = Window.partitionBy('carrier'),
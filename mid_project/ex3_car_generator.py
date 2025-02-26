from pyspark.sql import SparkSession
#from pyspark.sql.functions import monotonically_increasing_id, rand
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
million = 1000000

spark = SparkSession.builder.master('local').appName("CarsGenerator").getOrCreate()

temp_cars_df = spark.range(20)



cars_gen_df = temp_cars_df.select(
    (F.monotonically_increasing_id() + million).cast("int").alias("car_id"), 
    ((F.rand()*900*million + 100*million).cast("int")).alias("driver_id"),
    ((F.rand()*7 + 1).cast("int")).alias("model_id"),
    ((F.rand()*7 + 1).cast("int")).alias("color_id")
)

cars_gen_df.write.parquet('s3a://spark/data/dims/cars', mode='overwrite')

spark.stop()

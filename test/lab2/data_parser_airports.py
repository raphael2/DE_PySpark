from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



spark = SparkSession.builder.master("local").appName("ex2_airports").getOrCreate()

airport_raw_df = spark.read.csv('s3a://spark/data/raw/airports/', header=True)

airport_df = airport_raw_df.select(
                F.col('airport_id').cast(T.IntegerType()),
                F.col('city'),
                F.col('state'),
                F.col('name')
)

airport_df.show()

airport_df.write.parquet('s3a://spark/data/source/airports/', mode='overwrite')

spark.stop()
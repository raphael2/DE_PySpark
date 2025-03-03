from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer
import json
import time 


topic ='sensors-sample'
brokers = ['course-kafka:9092']
producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

""" json_schema = T.StructType([
    T.StructField('event_id', T.StringType()),
    T.StructField('event_time', T.TimestampType()),
    T.StructField('car_id', T.IntegerType()),
    T.StructField('speed', T.IntegerType()),
    T.StructField('rpm', T.IntegerType()),
    T.StructField('gear', T.IntegerType())
    ]) 
 """

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('DataGenerator') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()


cars = spark.read.parquet('s3a://spark/data/dims/cars')


cars_df = cars.select("car_id")\
    .withColumn("event_id",
                F.concat(
                    F.col("car_id").cast(T.StringType()),
                    F.lit('_'),
                    F.current_timestamp().cast(T.StringType())
                )
            )\
    .withColumn("event_time",F.current_timestamp())\
    .withColumn("speed",(F.rand()*F.lit(200)).cast(T.IntegerType()))\
    .withColumn("rpm",(F.rand()*F.lit(8000)).cast(T.IntegerType()))\
    .withColumn("gear",(F.rand()*F.lit(7)).cast(T.IntegerType()))
    

cars_df_js = cars_df.toJSON()
while True :

    for json_data in cars_df_js.collect():
        producer.send(topic, value=json_data)
        
        time.sleep(1)
            

producer.close()
spark.stop()

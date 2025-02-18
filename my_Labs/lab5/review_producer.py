import time
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer


# Topics/Brokers
topic ='gps-user-review-source'
brokers = ['course-kafka:9092']


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("ex5_reviews_producer")\
    .getOrCreate()

data_df  = spark.read.parquet('s3a://spark/data/source/google_reviews/')
data = data_df.toJSON()
print(data.take(6))

producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# The send() method creates the topic
#data = {'name':'avi','price':500,'target':'sergei'}
#producer.send(topic1, value=data)
#producer.flush()

i = 0
for json_data in data.collect():
    i = i + 1
    producer.send(topic, value=json_data)
    if i == 50:
        producer.flush()
        time.sleep(5)
        i = 0
producer.close()

spark.stop()
'''
Find the tables with the 5 highest percent of dessert buyers
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql import types as T
spark = SparkSession.builder.getOrCreate()

# Read a csv file
path = r'/home/developer/projects/spark-course-python/spark_course_python/my_Labs/lab2/my_exemples/dessert.csv'
dessert = spark.read.csv(path,header = True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')


dessert = dessert.withColumn('no_purchase', ~dessert.purchase)

#################################################

get_ratio_udf = F.udf(lambda row: row[0] / (row[0]+row[1]))


res = dessert\
    .groupby('table')\
    .agg(F.sum(F.col('purchase').cast(T.IntegerType())).alias('buyers'),
         F.sum(F.col('no_purchase').cast(T.IntegerType())).alias('non_buyers'))\
    .withColumn('ratio', get_ratio_udf(F.struct('buyers', 'non_buyers')))\
    .orderBy('ratio', ascending=False)
res.show(5)
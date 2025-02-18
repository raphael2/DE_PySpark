'''
Add to dessert a new column called 'no purchase' with the negative of 'purchse'.
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()

# Read a csv file
path = r'/home/developer/projects/spark-course-python/spark_course_python/my_Labs/lab2/my_exemples/dessert.csv'
dessert = spark.read.csv(path,header = True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')


dessert.withColumn('no purchase',~dessert.purchase).show()

spark.stop()
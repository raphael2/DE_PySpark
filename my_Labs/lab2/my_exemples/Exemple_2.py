'''
How many line we have in dataframe?

'''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dessert = spark.read.csv(r'/home/developer/projects/spark-course-python/spark_course_python/my_Labs/lab2/my_exemples/dessert.csv',header = True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

print(dessert.count())

 
spark.stop()
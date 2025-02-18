'''
How many groups purchased a dessert on Mondays?
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

path = r'/home/developer/projects/spark-course-python/spark_course_python/my_Labs/lab2/my_exemples/dessert.csv'
dessert = spark.read.csv(path,header = True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')


mask_1 = (dessert.weekday == 'Monday') & (dessert.purchase==True)
dessert.where(mask_1).show(5)
#dessert.show(5)
spark.stop()
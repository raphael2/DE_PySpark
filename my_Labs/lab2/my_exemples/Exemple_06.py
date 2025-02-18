'''
For each weekday - how many groups purchased a dessert?
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


dessert.where(dessert.purchase).groupBy('weekday').agg(F.sum(dessert.num_of_guests).alias('total_of_guests')).show()

#option 2 whithout import functions
# dessert= dessert\
#     .where(dessert.purchase)\
#     .groupBy('weekday')\
#     .agg({ 'num_of_guests': 'sum'})


spark.stop()

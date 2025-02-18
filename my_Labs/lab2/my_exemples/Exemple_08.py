'''
Capitalize the names of the shifts (e.g. noon  →  Noon)
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

new_dessert = dessert.withColumn('shift',F.initcap(dessert.shift))
new_dessert.show()
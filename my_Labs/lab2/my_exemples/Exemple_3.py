'''
How many groups purchased a dessert?
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


#exemple 1

print(dessert.filter(dessert.purchase).count())

#exemple 2
print(dessert.where(dessert.purchase).count())

# where == filter

spark.stop()
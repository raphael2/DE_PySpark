'''
How many visitors purchased a dessert?
What is the average table?
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

path = '/home/developer/projects/spark-course-python/spark_course_python/my_Labs/lab2/my_exemples/dessert.csv'

dessert = spark.read.csv(path,header = True,inferSchema = True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#dessert.where(dessert.purchase).count()


""" dessert\
    .where(dessert.purchase)\
    .agg({'num_of_guests': 'sum', 'table': 'mean'})\
    .show() """


#option1
# dessert\
#     .where(dessert.purchase)\
#     .agg({'num_of_guests': 'sum', 'table': 'mean'})\
#     .withColumnRenamed('sum(num_of_guests)', 'num_of_guests')\
#     .withColumnRenamed('avg(table)', 'avg_table')\
#     .show()

# option2
# import pyspark.sql.functions as sf

dessert\
    .where(dessert.purchase)\
        .agg(
            F.sum('num_of_guests').alias('num_of_guests')
            ,F.avg('num_of_guests').alias('avg_table')
        )\
        .show()

spark.stop()
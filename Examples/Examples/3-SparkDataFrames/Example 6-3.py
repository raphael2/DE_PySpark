'''
For each weekday - how many groups purchased a dessert?
bonus - plot the answer! (with function toPandas() )
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(r"/home/naya/Notebooks/dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#################################################

dessert= dessert\
    .where(dessert.purchase)\
    .groupBy('weekday')\
    .agg({ 'num_of_guests': 'sum'})

dessert.show()

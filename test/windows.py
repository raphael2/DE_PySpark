from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession \
 .builder \
 .master('local') \
 .config('spark.driver.memory', '4g') \
 .appName('ex4_anomalies_detection')\
 .getOrCreate()

df = spark.createDataFrame([(1, "A",100), (2, "A",200), (3, "A",300), (8, "A",400),(10,"A",500)], ["id", "category","value"])


Window_range = Window.partitionBy("category").orderBy("id").rangeBetween(-2,2)
#Window_rows = Window.partitionBy("category").orderBy("id").rowsBetween(-1,1)

# df = df.withColumn("sum_rows",F.sum("value").over(Window_rows))
df = df.withColumn("sum_range",F.sum("value").over(Window_range))
df.show()

spark.stop()
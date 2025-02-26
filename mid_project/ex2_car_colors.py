from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F


spark = SparkSession\
    .builder\
    .master("local")\
    .appName('ModelCreation')\
    .getOrCreate()


car_colors = [
    Row(color_id=1, color_name='Black'),
    Row(color_id=2, color_name='Red'),
    Row(color_id=3, color_name='Gray'),
    Row(color_id=4, color_name='White'),
    Row(color_id=5, color_name='Green'),
    Row(color_id=6, color_name='Blue'),
    Row(color_id=7, color_name='Pink'),
]
car_colors_df = spark.createDataFrame(car_colors)

car_colors_df.show()
car_colors_df.write.parquet('s3a://spark/data/dims/car_colors', mode='overwrite')


spark.stop()


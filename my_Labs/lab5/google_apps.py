from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("lab5_task1")\
    .getOrCreate()


google_apps_raw_df = spark.read.csv('s3a://spark/data/raw/google_apps/' ,header=True)

google_apps_temp_df = google_apps_raw_df.select(
        F.col('App').alias('application_name'),
        F.col('Category').alias('category'),
        F.col('Rating').alias('rating'),
        F.col('Reviews').alias('reviews'),
        F.col('Size').alias('size'),
        F.col('installs').alias('num_of_installs'),
        F.col('Price').alias('price')   
        ).fillna(-1, 'rating')


#google_apps_temp_df.show()
google_apps_temp_df.fillna(-1, 'rating').where(F.col('rating') == "-1").show()
""" google_apps_df = google_apps_raw_df.select(
        F.col('App').alias('application_name'),
        F.col('Category').alias('category'),
        F.col('Rating').alias('rating'),
        F.col('Reviews').cast(T.DoubleType()).alias('reviews'),
        F.col('Size').alias('size'),
        F.col('installs').cast(T.DoubleType()).alias('num_of_installs'),
        F.col('Price').cast(T.DoubleType()).alias('price')

        
        ) """

#google_apps_df.where(F.col('price')>0).show()
spark.stop()
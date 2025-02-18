from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]").appName('ex5_google_reviews').getOrCreate()

sentiment_rank_arr = [Row(Sentiment = "Positive", sentiment_rank = 1),
                 Row(Sentiment = "Neutral", sentiment_rank = 0),
                 Row(Sentiment = "Negative", sentiment_rank = -1)]



google_reviews_df = spark.read.csv('s3a://spark/data/raw/google_reviews/' ,header=True)

sentiment_rank_df = spark.createDataFrame(sentiment_rank_arr)
#sentiment_rank_df.show()



joined_df = google_reviews_df.join(F.broadcast(sentiment_rank_df), ['Sentiment'])
#joined_df.show()
selected_df = joined_df.select(
        F.col("App").alias("application_name"),
        F.col("Translated_review").alias("translated_review"),
        F.col("sentiment_rank").cast(T.LongType()),
        F.col("Sentiment_polarity").cast(T.FloatType()).alias("sentiment_polarity"),
        F.col("Sentiment_subjectivity").cast(T.FloatType()).alias("sentiment_subjectivity")
)
#selected_df.show()

selected_df.write.parquet('s3a://spark/data/source/google_reviews/' , mode='overwrite')

spark.stop()



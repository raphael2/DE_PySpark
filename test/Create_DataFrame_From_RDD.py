from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialisation de la SparkSession
spark = SparkSession.builder \
    .appName("ex1_word_count") \
    .master("local[*]") \
    .getOrCreate()

# Création d'un DataFrame à partir d'une liste de chaînes de caractères
text = ["""This is an example of spark application in Python
Python is a very common development language and it is also one of Spark's supported
languages
The library of Spark in Python called PySpark
In this example you will implement a word count application using PySpark
Good luck!!"""]

df = spark.createDataFrame(text, "string").toDF("line")


# Transformation: séparation des lignes en mots
df_words = df.select(explode(split(col("line"), "[ \n]")).alias("word"))

# Comptage des occurrences de chaque mot
df_word_count = df_words.groupBy("word").count()

# Collecte et affichage des résultats
df_word_count.show()

# Arrêt de la SparkSession
spark.stop()

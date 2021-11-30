import pyspark.sql.functions as fc
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("q2_b").getOrCreate()
film=spark.read.option("header","true").csv('film.csv')
film[film.length>=60].groupBy('rating').agg(fc.round(fc.mean('rental_rate'),2).alias('avg_rate'),fc.count('*').alias('cnt')).filter('cnt >= 160')[['rating','avg_rate']].show()

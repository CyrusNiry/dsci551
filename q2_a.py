import pyspark.sql.functions as fc
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("q2_a").getOrCreate()
film=spark.read.option("header","true").csv('film.csv')
film[(film.rating=='PG') | (film.rating=='PG-13')].agg(fc.count('*').alias('count')).show()

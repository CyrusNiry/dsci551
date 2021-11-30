import pyspark.sql.functions as fc
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("q2_b").getOrCreate()
film=spark.read.option("header","true").csv('film.csv')
film_actor=spark.read.option("header","true").csv('film_actor.csv')
actor=spark.read.option("header","true").csv('actor.csv')
join_df=film.join(film_actor,"film_id").join(actor,"actor_id")
filter_df=join_df[join_df.title=="ANONYMOUS HUMAN"]
filter_df[['first_name','last_name']].show()

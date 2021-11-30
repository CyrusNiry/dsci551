import pyspark.sql.functions as fc
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("q2_c").getOrCreate()
film_actor=spark.read.option("header","true").csv('film_actor.csv')
actor=spark.read.option("header","true").csv('actor.csv')
actor_id_most=film_actor.groupBy('actor_id').agg(fc.count('*').alias('cnt')).orderBy('cnt', ascending=False).limit(1)
join_df=actor.join(actor_id_most,'actor_id')
join_df[['first_name','last_name']].show()

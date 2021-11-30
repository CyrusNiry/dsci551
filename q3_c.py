from pyspark import SparkContext
from operator import add
sc=SparkContext(appName='dsci551 q3')
film_actor_lines=sc.textFile('film_actor.csv')
actor_lines=sc.textFile('actor.csv')

actor_actor_id_first_last_name=actor.map(lambda x: x.split(','))).map(lambda List: (List[0],[List[1],List[2]])).collect()
actor_film_actor_id_flim_id=actor.map(lambda x: x.split(','))).map(lambda List: (List[0],List[1])).collect()
actor_join_actor_film=actor_actor_id_first_last_name.join(actor_film_actor_id_flim_id)
Final_rdd=actor_join_actor_film.map(lambda List:(len(List[1]),List[1][0])).sortByKey(False).collect()
a=0
for i in Final_rdd:
    if (a==0):
print(i[1][0],i[1][1])
a+=1

from pyspark import SparkContext
from operator import add
sc=SparkContext(appName='dsci551 q3')
film_lines=sc.textFile('film.csv')
film_actor_lines=sc.textFile('film_actor.csv')
actor_lines=sc.textFile('actor.csv')
film_film_id_title=film_lines.map(lambda x: x.split(','))).map(lambda List: (List[0],List[1])).filter(lambda line: line[1]=='"ANONYMOUS HUMAN"').collect()
film_actor_film_id_actor_id=film_actor_lines.map(lambda x: x.split(','))).map(lambda List: (List[1],List[0])).collect()
film_actor_film_join=film_actor_film_id_actor_id.join(film_film_id_title).map(lambda List: (List[1][0],List[0])).collect()
film_actor_actor_id_film_id=film_actor_lines.map(lambda x: x.split(','))).map(lambda List: (List[0],List[1])).collect()
actor_actor_id_name=actor_lines..map(lambda x: x.split(','))).map(lambda List: (List[0],(List[1],List[2]))).collect()
final_join=actor_actor_id_name.join(film_actor_actor_id_film_id).map(lambda Line: Line[1][0]).collect()


for v in final_join:
print(v[0],v[1])

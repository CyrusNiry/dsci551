from pyspark import SparkContext
from operator import add
sc=SparkContext(appName='dsci551 q3')
film_lines=sc.textFile('film.csv')
count = film_lines.map(lambda x: x.split(',')).filter(lambda List: List[10]=='"PG"' or List[10]=='"PG-13"').map(lambda List: (List[10], 1)).count()
print(count)

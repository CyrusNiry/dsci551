from pyspark import SparkContext
from operator import add
sc=SparkContext(appName='dsci551 q3')
film_lines=sc.textFile('film.csv')
counts = film_lines.map(lambda x: x.split(',')).filter(lambda List: double(List[8].strip('"'))>=60).map(lambda List: (List[10], (1,double(List[7].strip('"'))))).reduceByKey(lambda a,b: (a[1]+b[1],a[0]+b[0])).filter(lambda a: a[1][1]>=160).map(lambda pair: pair[0],pair[1][0]/pair[1][1]).collect()
for i in counts:
    print(i[0],"{:.2f}".format(i[1]))

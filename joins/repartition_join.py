from pyspark.sql import SparkSession
from io import StringIO
import csv
import time
 
def split_sets(data):
    r = []
    g = []
    key = data[0]
    values = data[1] 
    for x in values:
        if x[0] == 'r':
            r.append(x[1:])
        else:
            g.append(x[1:])
    for rm in r:
        for gm in g:
            yield((key, (rm, gm)))

    #return ((key, (rm, gm)) for rm in r for gm in g)

f = open("times_join.txt", "a")
start = time.time()
spark = SparkSession.builder.appName('repartition-join').getOrCreate()

sc  =spark.sparkContext

genres = sc.textFile('hdfs://master:9000/files/genres_100.csv')\
            .map(lambda x: (x.split(",")[0], ('g', x.split(",")[1])))

ratings = sc.textFile('hdfs://master:9000/files/ratings.csv')\
            .map(lambda x: (x.split(",")[1], ('r', x.split(",")[0], *x.split(",")[2:])))

rdd = ratings.union(genres)


result = rdd.groupByKey().\
        flatMap(lambda x: split_sets(x))


result.saveAsTextFile('hdfs://master:9000/outputs/repartition_join.csv')

end = time.time() - start
f.write(str(end) + '\n')
f.close()

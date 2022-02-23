from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

f = open("times_join.txt", "a")

start = time.time()

spark = SparkSession.builder.appName('broadcast-join').getOrCreate()

sc  =spark.sparkContext

def buildMap(broadcasted):
    return dict(broadcasted)

genres = sc.textFile('hdfs://master:9000/files/genres_100.csv')

ratings = sc.textFile('hdfs://master:9000/files/ratings.csv').\
          map(lambda x: (x.split(",")[1], (x.split(",")[0], *x.split(",")[2:])))

genres = genres.map(lambda x: (x.split(",")[0], x.split(",")[1])).\
                groupByKey().\
                collect()

broadcast_genres = sc.broadcast(genres)


result = ratings.flatMap(lambda x: [(x[0], (*x[1:], genre)) for genre in buildMap(broadcast_genres.value).get(x[0], [])])

result.saveAsTextFile('hdfs://master:9000/outputs/broadcast_join.csv')


end = time.time() - start
f.write(str(end) + '\n')
f.close()

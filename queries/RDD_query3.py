from pyspark.sql import SparkSession
from io import StringIO
import csv
import time 

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('RDD_query3').getOrCreate()

sc = spark.sparkContext


genres = sc.textFile('hdfs://master:9000/files/movie_genres.csv'). \
        map(lambda x: (x.split(',')[0], x.split(',')[1])). \
        groupByKey()


ratings = sc.textFile('hdfs://master:9000/files/ratings.csv'). \
        map(lambda x: (x.split(',')[1], (float(x.split(',')[2]), 1))). \
        reduceByKey(lambda avg, x: (avg[0] + x[0], avg[1] + x[1])). \
        map(lambda x: (x[0], x[1][0] / x[1][1]))


result = genres.join(ratings).\
        flatMap(lambda x: [(eidos, (x[1][1], 1)) for eidos in x[1][0]]). \
        reduceByKey(lambda avg, x: (avg[0] + x[0], avg[1] + x[1])). \
        map(lambda x: (x[0], ((x[1][0] / x[1][1]), x[1][1])))

result.saveAsTextFile('hdfs://master:9000/outputs/RDD_query3.csv')

end = time.time() - start
f.write(str(end) + "\n")
f.close()

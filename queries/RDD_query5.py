from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('RDD_query5').getOrCreate()

sc  =spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


genres = sc.textFile('hdfs://master:9000/files/movie_genres.csv'). \
        map(lambda x: (x.split(',')[0], x.split(',')[1]))

#(movied_id, genre)

movies = sc.textFile('hdfs://master:9000/files/movies.csv'). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (x[0], (x[1], float(x[7]))))

#(movie_id, (name, popularity))

ratings = sc.textFile('hdfs://master:9000/files/ratings.csv'). \
          map(lambda x: (x.split(',')[1], (x.split(',')[0], float(x.split(',')[2]))))

#(movied_id, (user_id, rating))

result1 = genres.join(ratings). \
        map(lambda x: ((x[1][0], x[1][1][0]), (x[1][1][1], x[0]))). \
        groupByKey(). \
        map(lambda x: (x[0][0], (x[0][1], x[1], len(x[1])))). \
        reduceByKey(lambda maxv, x: x if x[2] > maxv[2] else maxv). \
        flatMap(lambda x: [(tainia, (kritiki, x[0], x[1][0], x[1][2])) for kritiki, tainia in x[1][1]]). \
        join(movies). \
        map(lambda x: ((x[1][0][1], x[1][0][2], x[1][0][3]), (x[1][1][0], x[1][0][0], x[1][1][1])))

#((genre, user, plithos), (name, rating, pop))

result2 = result1. \
        reduceByKey(lambda best, x: x if (x[1], x[2]) > (best[1], best[2]) else best)

#((genre, user, plithos), (name, rating))        

result3 = result1. \
        reduceByKey(lambda worst, x: x if x[1] < worst[1] else(x if x[1] == worst[1] and x[2] > worst[2] else worst))

#((genre, user, plithos), (name, rating))

final = result2.join(result3). \
        sortByKey(). \
        map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))
#(genre, user, plithos, bestname, bestrating, worstname, worstrating)

final.saveAsTextFile('hdfs://master:9000/outputs/RDD_query5.csv')
                
end = time.time() - start
f.write(str(end)+"\n")
f.close()



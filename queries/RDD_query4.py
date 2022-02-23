from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('RDD_query4').getOrCreate()

sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def five_year_period(date):
    if date:
        date = int(date[:4])
        if  2000 <= date <= 2004:
            return '2000-2004'
        elif 2005 <= date <= 2009:
            return '2005-2009'
        elif 2010 <= date <= 2014:
            return '2010-2014'
        elif 2015 <= date <= 2019:
            return '2015-2019'
        else:
            return None
    else:
        return None

genres = sc.textFile('hdfs://master:9000/files/movie_genres.csv'). \
        map(lambda x: (x.split(',')[0], x.split(',')[1])). \
        filter(lambda x: x[1] == 'Drama')


movies = sc.textFile('hdfs://master:9000/files/movies.csv'). \
        map(lambda x: split_complex(x)). \
        filter(lambda x: x[2]) .\
        map(lambda x: (x[0], (len(x[2].split(' ')), five_year_period(x[3])))). \
        filter(lambda x: x[1][1]) 

result = genres.join(movies). \
         map(lambda x: (x[1][1][1], (x[1][1][0], 1))). \
         reduceByKey(lambda avg, x: (avg[0] + x[0], avg[1] + x[1])). \
         map(lambda x: (x[0], x[1][0] / x[1][1]))

result.saveAsTextFile('hdfs://master:9000/outputs/RDD_query4.csv')

end=time.time()-start
f.write(str(end)+"\n")
f.close()

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

f = open('times.txt', 'a')
start = time.time()

spark = SparkSession.builder.appName('RDD_query1').getOrCreate()

sc  =spark.sparkContext

def getYear(datetime):
    if datetime:
        return datetime[:4]
    else:
        return None

result = sc.textFile('hdfs://master:9000/files/movies.csv'). \
        map(lambda x: split_complex(x)). \
        map(lambda x: (getYear(x[3]), (x[1], float(x[5]), float(x[6])))). \
        filter(lambda x: x[0] != None and x[1][1] != 0 and x[1][2] != 0 and int(x[0]) >= 2000). \
        map(lambda x: (x[0], (x[1][0], (x[1][2] - x[1][1]) / x[1][1] * 100))). \
        reduceByKey(lambda max, x: max if max[1] > x[1] else x)

result.saveAsTextFile('hdfs://master:9000/outputs/RDD_query1.csv')

end = time.time() - start
f.write(str(end) + '\n')
f.close()

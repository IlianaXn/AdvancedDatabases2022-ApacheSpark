from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

f = open("times.txt", "a")

start = time.time()

spark = SparkSession.builder.appName('RDD_query2').getOrCreate()

sc  =spark.sparkContext

result = sc.textFile('hdfs://master:9000/files/ratings.csv'). \
         map(lambda x: (x.split(',')[0], (float(x.split(',')[2]), 1))). \
         reduceByKey(lambda sum_count, x: (sum_count[0] + x[0], sum_count[1] + x[1])). \
         map(lambda x: (x[0], x[1][0] / x[1][1])) .\
         map(lambda x: ('result', (1, 1)) if x[1] > 3.0 else ('result', (1, 0))).\
         reduceByKey(lambda sums, x: (sums[0] + x[0], sums[1] + x[1])).\
         map(lambda x: x[1][1] / x[1][0] * 100)

result.saveAsTextFile('hdfs://master:9000/outputs/RDD_query2.csv')

end = time.time() - start
f.write(str(end) + '\n')
f.close()

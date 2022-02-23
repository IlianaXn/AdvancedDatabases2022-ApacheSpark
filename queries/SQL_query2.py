from pyspark.sql import SparkSession
import time
import sys

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('SQL_query2').getOrCreate()

parquet = sys.argv[1]
if parquet == 'F':
    ratings = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/ratings.csv')
elif parquet == 'T':
    ratings = spark.read.parquet('hdfs://master:9000/files/ratings.parquet')
else:
    raise Exception ("This setting is not available.")

ratings = ratings.withColumnRenamed('_c0', 'user')
ratings = ratings.withColumnRenamed('_c2', 'rate')

ratings.registerTempTable('ratings')

query = 'select count(*)/(select count(distinct user) from ratings) * 100.0 as percentage \
        from (select user, avg(rate) as rate \
        from ratings \
        group by user \
        having rate > 3.0) avg_rate'


res = spark.sql(query)

if parquet == 'F':
    res.write.csv('hdfs://master:9000/outputs/SQL_query2.csv')
elif parquet == 'T':
    res.write.csv('hdfs://master:9000/outputs/SQL_query2_parquet.csv')

end = time.time() - start
f.write(str(end)+"\n")
f.close()


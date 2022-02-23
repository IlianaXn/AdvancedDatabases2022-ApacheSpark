from pyspark.sql import SparkSession
import time
import sys

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('SQL_query3').getOrCreate()

parquet = sys.argv[1]
if parquet == 'F':
    ratings = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/ratings.csv')
    genres = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movie_genres.csv')
elif parquet == 'T':
    ratings = spark.read.parquet('hdfs://master:9000/files/ratings.parquet')
    genres = spark.read.parquet('hdfs://master:9000/files/movie_genres.parquet')
else:
    raise Exception ("This setting is not available.")

ratings = ratings.withColumnRenamed('_c1', 'movie_id')
ratings = ratings.withColumnRenamed('_c2', 'rating')

genres = genres.withColumnRenamed('_c0', 'movie_id')
genres = genres.withColumnRenamed('_c1', 'genre')


ratings.registerTempTable('ratings')
genres.registerTempTable('genres')

query = 'select genre, avg(avg_rate) as avg_rating, count(*) as amount\
         from \
         (select movie_id, avg(rating) as avg_rate \
         from ratings \
         group by movie_id) rates natural join genres \
         group by genre'


res = spark.sql(query)

if parquet == 'F':
    res.write.csv('hdfs://master:9000/outputs/SQL_query3.csv')
elif parquet == 'T':
    res.write.csv('hdfs://master:9000/outputs/SQL_query3_parquet.csv')

end = time.time() - start
f.write(str(end)+"\n")
f.close()

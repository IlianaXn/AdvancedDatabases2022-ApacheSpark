from pyspark.sql import SparkSession
import time
import sys

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('SQL_query1').getOrCreate()

parquet = sys.argv[1]
if parquet == 'F':
    movies = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movies.csv')
elif parquet == 'T':
    movies = spark.read.parquet('hdfs://master:9000/files/movies.parquet')    
else:
    raise Exception ("This setting is not available.")

movies = movies.withColumnRenamed('_c1', 'title')
movies = movies.withColumnRenamed('_c3', 'release_date')
movies = movies.withColumnRenamed('_c5', 'cost')
movies = movies.withColumnRenamed('_c6', 'gains')

movies.registerTempTable('movies')

query = 'select max_gain.year, gain_per_movie.title, max_gain.kerdos \
        from (select year(release_date) as year, max((gains - cost) / cost * 100) as kerdos \
                from movies where release_date is not null and cost <> 0 and gains <> 0 and year(release_date) >=2000 group by year) max_gain \
                natural join (select year(release_date) as year,  title, ((gains -cost) / cost * 100) as kerdos \
                from movies where release_date is not null and cost <>0 and gains <>0) gain_per_movie'


res = spark.sql(query)

if parquet == 'F':
    res.write.csv('hdfs://master:9000/outputs/SQL_query1.csv')
elif parquet == 'T':
    res.write.csv('hdfs://master:9000/outputs/SQL_query1_parquet.csv')

end = time.time() - start
f.write(str(end)+"\n")
f.close()

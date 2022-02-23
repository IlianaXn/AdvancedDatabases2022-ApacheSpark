from pyspark.sql import SparkSession
import sys, time

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('SQL_query4').getOrCreate()

parquet = sys.argv[1]
if parquet == 'F':
    movies = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movies.csv')
    genres = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movie_genres.csv')
elif parquet == 'T':
    movies = spark.read.parquet('hdfs://master:9000/files/movies.parquet')
    genres = spark.read.parquet('hdfs://master:9000/files/movie_genres.parquet')
else:
    raise Exception ("This setting is not available.")

def five_year_period(date):
    if  2000 <= date <= 2004:
        return '2000-2004'
    elif 2005 <= date <= 2009:
        return '2005-2009'
    elif 2010 <= date <= 2014:
        return '2010-2014'
    elif 2015 <= date <= 2019:
        return '2015-2019'
        

#udfs require more time than sql functions, use when not applicable through SQL code

spark.udf.register('fyear', five_year_period)

movies = movies.withColumnRenamed('_c0', 'movie_id')
movies = movies.withColumnRenamed('_c2', 'descr')
movies = movies.withColumnRenamed('_c3', 'release_date')

genres = genres.withColumnRenamed('_c0', 'movie_id')
genres = genres.withColumnRenamed('_c1', 'genre')


movies.registerTempTable('movies')
genres.registerTempTable('genres')



query = 'select fyear(year(release_date)) as period, avg(length(descr) - length(replace(descr," ","")) + 1) as avg_descr\
         from movies natural join genres \
         where year(release_date) > 1999 and year(release_date) < 2020 and release_date is not null and descr is not null and genre = "Drama" \
         group by period'

res = spark.sql(query)

if parquet == 'F':
    res.write.csv('hdfs://master:9000/outputs/SQL_query4.csv')
elif parquet == 'T':
    res.write.csv('hdfs://master:9000/outputs/SQL_query4_parquet.csv')

end=time.time()-start
f.write(str(end)+"\n")
f.close()


from pyspark.sql import SparkSession
import sys, time

f = open("times.txt","a")
start = time.time()

spark = SparkSession.builder.appName('SQL_query5').getOrCreate()

parquet = sys.argv[1]
if parquet == 'F':
    movies = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movies.csv')
    genres = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/movie_genres.csv')
    ratings = spark.read.format('csv'). \
        options(header='false', inferSchema='true'). \
        load('hdfs://master:9000/files/ratings.csv')
elif parquet == 'T':
    movies = spark.read.parquet('hdfs://master:9000/files/movies.parquet')
    genres = spark.read.parquet('hdfs://master:9000/files/movie_genres.parquet')
    ratings = spark.read.parquet('hdfs://master:9000/files/ratings.parquet')
else:
    raise Exception ("This setting is not available.")

movies = movies.withColumnRenamed('_c0', 'movie_id')
movies = movies.withColumnRenamed('_c1', 'name')
movies = movies.withColumnRenamed('_c7', 'popularity')

genres = genres.withColumnRenamed('_c0', 'movie_id')
genres = genres.withColumnRenamed('_c1', 'genre')


ratings = ratings.withColumnRenamed('_c0', 'user_id')
ratings = ratings.withColumnRenamed('_c1', 'movie_id')
ratings = ratings.withColumnRenamed('_c2', 'rating')

movies.registerTempTable('movies')
genres.registerTempTable('genres')
ratings.registerTempTable('ratings')


query = 'select genre, user_id, first(plithos) as plithos, \
        first(best_n) as best_n, first(best_r) as best_r, first(worst_n) as worst_n, \
        first(worst_r) as worst_r \
        from \
        (select genre, user_id, plithos, first(name) over w_best as best_n, \
        first(rating) over w_best as best_r, first(name) over w_worst as worst_n, \
        first(rating) over w_worst as worst_r \
        from \
        (select movie_id, genre, user_id, rating, plithos \
        from \
        (select movie_id, genre, user_id, rating, plithos, max(plithos) over (partition by genre) as max_plithos from \
        (select movie_id, genre, user_id, rating, count(rating) over (partition by genre, user_id) as plithos\
        from ratings natural join genres)) \
        where plithos = max_plithos) natural join movies \
        window w_best as (partition by genre, user_id order by rating desc, popularity desc), \
        w_worst as (partition by genre, user_id order by rating asc, popularity desc)) \
        group by genre, user_id \
        order by genre'

res = spark.sql(query)

if parquet == 'F':
    res.write.csv('hdfs://master:9000/outputs/SQL_query5.csv')
elif parquet == 'T':
    res.write.csv('hdfs://master:9000/outputs/SQL_query5_parquet.csv')

end = time.time() - start
f.write(str(end)+"\n")
f.close()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("covert-csv2parquet").getOrCreate()

#load csvs
movies = spark.read.csv("hdfs://master:9000/files/movies.csv", header=False, inferSchema=True)

genres = spark.read.csv("hdfs://master:9000/files/movie_genres.csv", header=False, inferSchema=True)

ratings = spark.read.csv("hdfs://master:9000/files/ratings.csv", header=False, inferSchema=True)

#save as parquet
movies.write.parquet("hdfs://master:9000/files/movies.parquet")
genres.write.parquet("hdfs://master:9000/files/movie_genres.parquet")
ratings.write.parquet("hdfs://master:9000/files/ratings.parquet")

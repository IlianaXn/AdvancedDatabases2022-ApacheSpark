from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('covert1').getOrCreate()

sc  =spark.sparkContext

result = sc.textFile('hdfs://master:9000/files/movie_genres.csv'). \
        take(100)

result = sc.parallelize(result)

result.saveAsTextFile('hdfs://master:9000/files/genres_100.csv')

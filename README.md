# AdvancedDatabases2022-ApacheSpark
This project was developed by two undergraduate students, [George Kallitsis](https://github.com/giorgoskallitsis99) and [Iliana Xygkou](https://github.com/IlianaXn) for the course Advanced Topics in Database Systems 2021-2022 [NTUA ECE](https://www.ece.ntua.gr/gr).
Its purpose is to evaluate the performance of the fundamental APIs of [Apache Spark](https://spark.apache.org/), [RDD API](https://spark.apache.org/docs/latest/rdd-programming-guide.html) and [Dataframe API / Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) (using SQL queries).

## Dataset

For our project a subset of the [Full MovieLens Dataset](https://grouplens.org/datasets/movielens/latest/) was used, and it can be found [here](http://www.cslab.ntua.gr/courses/atds/movie_data.tar.gz).

There are given 3 *csv* files:
*  movies.csv (17MB): Primary key is ___movie_id___.
```
movie_id, movie_title, description, release_date(timestamp), duration(min), production_cost, revenue, popularity
Example:
862,Toy Story,Led by Woody Andys toys live happily in his room until Andys birthday brings 
Buzz Lightyear onto the scene Afraid of losing his place in Andys heart Woody plots against Buzz 
But when circumstances separate Buzz and Woody from their owner the duo eventually learns to 
put aside their differences,1995-10-30T00:00:00.000+02:00,81.0,30000000,373554033,21.946943
```
*  movie_genres.csv (1.3 MB): Primary key is ___(movie_id, genre)___.
```
movie_id, genre
Example:
862, Animation
```
*  ratings.csv (677 MB): Primary key is ___(user_id, movie_id)___.
```
user_id, movie_id, rating, timestamp
Example:
1,110,1.0,1425941529
```
We uploaded these files in our [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) by executing the following commands:
<pre>
hadoop fs -mkdir hdfs://master:9000/files
hadoop fs -put <i>file</i>.csv hdfs://master:9000/files/
</pre>

## Queries

First, we note that in order to execute the *Python* scripts for this project the appropriate command is the following:
<pre>
spark-submit <i>script</i>.py [argument]
</pre>

We executed 5 queries using:
1. Map Reduce Queries – RDD API 
2. Spark SQL with input the *csv* file
3. Spark SQL with input the *parquet* file

The [*parquet*](https://parquet.apache.org/) files were produced from the corresponding *csv* files by executing the *convert.py*.

**Query 1**:
Since 2000, for each year find the movie with the biggest profit ((*revenue - production_cost) / production_cost* * 100). Ignore any records missing *release_date*, or having zero values in *production_cost* or *revenue*.

**Query 2**:
Find the percentage of users that have rated movies with average score exceeding 3.

**Query 3**:
For each genre find its average rating ( (sum_movies((1 / #*rating* * sum(*rating*))) / #movies_in_genre ) and the number of movies belonging to it. If a movie belongs to more than one genre, it is counted in each genre. 

**Query 4**:
For the movies belonging to the "Drama" genre find the average length of movie's description in words per lustrum since 2000 (i.e., 2000-2004, 2005-2009, 2010-2014, 2015-2019). Ignore any records missing *description*.

**Query 5**:
For each genre find the user with the greatest number of ratings, their most favorite movie and their least favorite movie according to their ratings. In case a user has the same highest / lowest rating in more than 1 movies, choose the most popular one among those. The results should be sorted according to the *genre* attribute and each row should contain the following:
* *genre*
* *user_id* with most ratings
* *number_of_ratings*
* *most_favorite_movie_title*
* *rating_of_most_favorite_movie*
* *least_favorite_movie_title*
* *rating_of_least_favorite_movie*

The implementation of all 5 queries using Map-Reduce and Spark SQL is contained in the *queries* directory.

The results concerning the execution time are presented below and are explained in *report.pdf* (in greek, we're sorry for that).

![Alt text](/outputs/queries.png?raw=true "Queries Execution Times")

## Joins

We also evaluated the performance of the different implementations of ___join___ in Spark's Map-Reduce context:
* Repartition join (or Reduce-Side join)
* Broadcast join (or Map-Side join)

Their description and Map-Reduce pseudocode can be found in the paper "A Comparison of Join Algorithms for Log Processing in
MapReduce" [^ref1]. Their implementation is given in the *joins* directory.

We evaluated their perfomarmance by joining the last 100 lines of *genres.csv* with *ratings.csv*.

The results concerning the execution time are presented below and are explained in *report.pdf*.

![Alt text](/outputs/joins.png?raw=true "Joins Execution Times")

Finally, we evaluated the use of the Catalyst Optimizer by Spark SQL. In more details, by executing *optimizer.py* in the *joins* directory, we compared the performance of join with or without the activation of the optimizer. The latter could be achieved by setting the variable *spark.sql.autoBroadcastJoinThreshold* to -1.

The results concerning execution time are presented below and are explained in *report.pdf*.

![Alt text](/outputs/optimizer.png?raw=true "Optimizer Execution Times")


[^ref1]:   S. Blanas, J. M. Patel, V. Ercegovac, J. Rao, E. J. Shekita, and Y. Tian, “A comparison of join algorithms for log processing in MapReduce,” in Proceedings of the. 2010 ACM Int. Conf. Management of Data (SIGMOD’10), pp. 975–986, Indianapolis Indiana, USA, June 2010

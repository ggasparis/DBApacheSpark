from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('query3-rdd').getOrCreate()

sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

################################################################################
#################################### Q3 ########################################
################################################################################

def q3_rdd():

    t1 = time.time()

	# in rat rdd read the ratings data
	# map the pair of movieID(key) with rating(value) that appears once
	# return for every movie the sum of reviews and number of reviews
	# map the average rating
	# in gen rdd read movie_genres data
	# map the pair of movieID(key) and Genres(value)
	# in all rdd join rat  and gen with movieID(key)
	# for each genre return the sum of average ratrings of movie and the number of movies
	# return the average rating per genre
	# in sorted rdd format and sort per genre
	# write results

    rat = \
		sc.textFile('hdfs://master:9000/project/ratings.csv'). \
    	map(lambda x : split_complex(x)). \
    	map(lambda x : (x[1], (1, float(x[2])))). \
		reduceByKey(lambda x, y: (x[0]+y[0], x[1], x[2]+y[2])). \
    	mapValues(lambda x : x[1]/x[0])

    gen = \
		sc.textFile('hdfs://master:9000/project/movie_genres.csv'). \
    	map(lambda x : split_complex(x)). \
    	map(lambda x : (x[0], x[1]))

    all = \
		rat.join(gen). \
    	map(lambda x : (x[1][1], (1, float(x[1][0])))). \
		reduceByKey(lambda x, y: (x[0]+y[0], x[1], x[2]+y[2])). \
    	mapValues(lambda x : (x[0], x[1]/x[0]))

	sorted = \
		all.map(lambda x : (x[0], x[1][1], x[1][0])).coalesce(1, True).sortBy(lambda x : x[0])

    res = \
		sorted.saveAsTextFile('hdfs://master:9000/project/output/query3-rdd')

    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q3_rdd()

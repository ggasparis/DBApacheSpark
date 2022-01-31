from pyspark.sql import SparkSession
from io import StringIO
import time
import csv

spark = SparkSession.builder.appName("query4-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

################################################################################
#################################### Q4 ########################################
################################################################################

def  q4_rdd():

    t1 = time.time()

	#  in movies rdd map the dataset with the pair of movieID(key) and values of
	# the pair of summary and the year (values)
	# in movies_genres read the movie_genres dataset and map all the movieID that
	# has the genre Drama
	# in gen_drama rdd join the movies and movie_genres tables on movieID(key)
	# aggregate on dates and calculate the number of summaries and their word count 
	# and the average summary per year
	# count for every 5 yeas
	# create new key and calulcate sum of word count
	# calculate the average summary for the 5 years period
	# aggregate the rdds and write results

    movies = \
        sc.textFile('hdfs://master:9000/project/movies.csv'). \
    	map(lambda x : split_complex(x)). \
    	filter(lambda x : x[3] != ""). \
    	filter(lambda x : int(x[3].split('-')[0]) >= 2000). \
    	map(lambda x : (x[0], (x[2], x[3].split('-')[0])))


    movie_genres= \
        sc.textFile('hdfs://master:9000/project/movie_genres.csv'). \
    	map(lambda x : split_complex(x)). \
    	filter(lambda x : x[1] == 'Drama'). \
    	map(lambda x : (x[0], x[1]))


    gen_drama = \
        movies.join(movie_genres). \
    	map(lambda x : (x[1][0][1], (1, len(x[1][0][0].split(" ")) if x[1][0][0] else 0))). \
        reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])). \
    	mapValues(lambda x : x[1]/x[0])

    res = []
    period = ['2000-2004', '2005-2009', '2010-2014', '2015-2019']

    for i in period:
    	q = \
			gen_drama.filter(lambda x : int(x[0]) < int(i.split('-')[1])+1 and int(x[0]) > int(i.split('-')[0])-1). \
    		map(lambda x : (i, (1, x[1]))). \
        	reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])). \
    		mapValues(lambda x : x[1]/x[0])

    	res.append(q)

    res = \
		sc.union(res). \
    	coalesce(1, True). \
		sortBy(lambda x : x[0]). \
		saveAsTextFile('hdfs://master:9000/project/output/query4-rdd')

    t2 =   time.time()
    print("Time:", t2-t1, "secs")

    return t2-t1

if __name__ == "__main__":
    q4_rdd()

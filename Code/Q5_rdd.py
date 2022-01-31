from pyspark.sql import SparkSession
from io import StringIO
import time
import csv

spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

################################################################################
#################################### Q5 ########################################
################################################################################

def q5_rdd():

    t1 = time.time()


    ratings = \
		sc.textFile('hdfs://master:9000/project/ratings.csv'). \
		map(lambda x : split_complex(x)). \
		map(lambda x : (x[1], (x[0], x[2])))

    movie_genres = \
		sc.textFile('hdfs://master:9000/project/movie_genres.csv'). \
    	map(lambda x : split_complex(x))

	movies = \
		sc.textFile('hdfs://master:9000/project/movies.csv'). \
    	map(lambda x : split_complex(x)). \
    	map(lambda x : (x[0], (x[1], x[7])))



    rat_gens =
		ratings \
    	join(movie_genres). \
    	map(lambda x : ((x[1][1], x[1][0][0]), 1)). \
    	reduceByKey((lambda x, y : x+y), (lambda a, b : a+b))

    gen_users =
		rat_gens\
    	map(lambda x : (x[0][0], (x[0][1], int(x[1])))). \
    	reduceByKey(lambda x, y : (x[0] if x[1] >= y[1] else y[0], max(x[1], y[1]))). \ # max pair 
    	map(lambda x : (x[0], (x[1][0], x[1][1])))

    rat_gens_2 = \
		rat_gens.map(lambda x : ((x[0][0], x[1]), x[0][1]))

    gen_users_2 =
		gen_uers \
    	map(lambda x : ((x[0], x[1][1]), x[1][0])). \
    	join(rat_gens_2). \
    	map(lambda x : (x[0][0], (x[1][1], x[0][1])))


    rat =
		ratings \
    	map(lambda x : ((x[1][0], x[0]), x[1][1]))


    all =
		movie_genres\
    	map(lambda x : (x[1], x[0])). \
    	join(gen_users). \
    	map(lambda x : (x[1][0], (x[0], x[1][1][0], x[1][1][1]))). \
    	join(movies). \
    	map(lambda x : ((x[1][0][1], x[0]), (x[1][0][0], x[1][0][2], x[1][1][0], x[1][1][1]))). \
    	join(rat). \
    	map(lambda x : (x[1][0][0], x[0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1]))


    all =
		all \
    	map(lambda x : ((x[0], x[1], x[5]), (x[2], x[3], x[4])))

    ratMax =
		all \
    	map(lambda x : ((x[0], x[1]), x)). \
    	reduceByKey(lambda x, y : max(x, y, key=lambda x : x[5])). \
    	map(lambda x : ((x[0][0], x[0][1], x[1][5]), x[1][5])). \
    	join(all_2). \
    	map(lambda x : ((x[0][0], x[0][1], x[0][2]), (x[1][1][0], x[1][1][1], x[1][1][2]))). \
    	reduceByKey(lambda x, y : max(x, y, key=lambda x : float(x[2]))). \
    	map(lambda x : ((x[0][0], x[0][1], x[1][0]), (x[1][1], x[1][2], x[0][2])))


    ratMin =
		all \
    	map(lambda x : ((x[0], x[1]), x)). \
    	reduceByKey(lambda x, y : min(x, y, key=lambda x : x[5])). \
    	map(lambda x : ((x[0][0], x[0][1], x[1][5]), x[1][5])) \
    	join(all_2). \
    	map(lambda x : ((x[0][0], x[0][1], x[0][2]), (x[1][1][0], x[1][1][1], x[1][1][2]))). \
    	reduceByKey(lambda x, y : max(x, y, key=lambda x : float(x[2]))). \
    	map(lambda x : ((x[0][0], x[0][1], x[1][0]), (x[1][1], x[1][2], x[0][2])))


    res =
		ratMax \
    	join(ratMin) \
    	map(lambda x : (x[0][0], x[0][1], x[0][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][0], x[1][1][1], x[1][1][2])). \
    	coalesce(1, True). \
		sortBy(lambda x : x[0]). \
		saveAsTextFile('hdfs://master:9000/project/output/query5-rdd')

    t2 =   time.time()

    print("Time:", t2-t1, "secs")
    return t2-t1

if __name__ == "__main__":
    q5_rdd()

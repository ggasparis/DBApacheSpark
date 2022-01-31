from io import StringIO
from pyspark.sql import SparkSession
import csv
import time

spark = SparkSession.builder.appName('query2-rdd').getOrCreate()

sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

################################################################################
#################################### Q2 ########################################
################################################################################
def q2_rdd():

    t1 = time.time()

	# map every user(key) with rating(value) with keeping only the 1st digit
	# merge the ratings(values) for each user(key)
	# calculate users percantage(value) and pass it in the user-rating pairs
	# filter: return a new RDD with the ratings above 3
	# return a list with all the elements of rdd
	# collect a list with all the ratings
	# calculate total percenage in new RDD
	# create a new rdd from perc
	
    rat = \
		sc.textFile('hdfs://master:9000/project/ratings.csv'). \
		map(lambda x: (int(split_complex(x)[0]), (1, float(split_complex(x)[2])))). \
		reduceByKey(lambda x,y: (x[0] + y[0],x[1] + y[1])). \
    	mapValues(lambda x : x[1]/x[0])

    raThree = \
		rat.filter(lambda x : x[1] > 3). \
		collect()

    rat = \
		rat.collect()

    perc = round(len(raThree)/len(rat), 2)
    res = sc.parallelize([perc]).coalesce(1, True).saveAsTextFile('hdfs://master:9000/project/output/query2-rdd')

    t2 = time.time()
    print("Time:",t2 - t1,"secs")

    return t2 - t1

if __name__ == "__main__":
    q2_rdd()

from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

if __name__ == "__main__":

	spark = SparkSession.builder.appName('BrJoin').getOrCreate()

	sc = spark.sparkContext

	def BrJoin(comb):
		if comb[0] in br_res.value.keys():
			return (comb[0], (comb[1], br_res.value[comb[0]]))

	R = \
		sc.textFile('hdfs://master:9000/project/movie_genres_new.csv'). \
		map(lambda x : split_complex(x)). \
		map(lambda x : (x[0], x[1]))


	t1 = time.time()

	res = {} #init() state of accumulator

	Rl = R.collect()
	for n in Rl:
		if n[0] in res.keys():
			res[n[0]].append(n[1])
		else:
			res[n[0]] = [n[1]]

	for key, value in res.items():
		res[key] = tuple(value)
		
	br_res = sc.broadcast(res)

	L = \
		sc.textFile('hdfs://master:9000/project/ratings.csv'). \
		map(lambda x : split_complex(x)). \
		map(lambda x : (x[1], (x[0], x[2], x[3]))). \
		map(lambda x : BrJoin(x)). \
		filter(lambda x : x is not None). \
		collect()


	t2 = time.time()
	print("time:",t2 - t1, "secs")

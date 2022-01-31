from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('RepartitionJoin').getOrCreate()

sc = spark.sparkContext

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

def repJoin(v1, v2):
# v1 & v2 for rdds

    def fun(values):
        L = []
        R = []
        for (n, v) in values:
            if n == 'L':
                L.append(v)
            elif n == 'R':
                R.append(v)
        return ((l, r) for l in L for r in R)

	L = v1.mapValues(lambda v: ('L', v))
    R = v2.mapValues(lambda v: ('R', v))

    return L.union(R).groupByKey().flatMapValues(lambda x: fun(x))


movie_genres =  sc.textFile('hdfs://master:9000/project/movie_genres_new.csv'). \
                map(lambda x : split_complex(x)). \
                map(lambda x : (x[0],x[1]))

ratings =   sc.textFile('hdfs://master:9000/project/ratings.csv'). \
            map(lambda x : split_complex(x)). \
            map(lambda x : (x[1], (x[0])))


t1 = time.time()

res = repJoin(ratings,movie_genres).collect()

t2 = time.time()

print("time:" t2 - t1, "secs")

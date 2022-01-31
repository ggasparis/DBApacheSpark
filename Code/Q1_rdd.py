from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('query1-rdd').getOrCreate()

sc = spark.sparkContext

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

################################################################################
#################################### Q1 ########################################
################################################################################
def q1_rdd():

    t1 = time.time()

    # map the rows of the dataset with the function split_complex
    # filter the dataset and check the conditions of the  columns we need
    # reduce funcction given the pair of cost and revenue
    # and return the pair with the biggest profit
    # map the year, title and "rounded" profit from the dtaset
    # coalesce function to shuffle to one partition
    # sort by the year
    # save the output as text file
    
    res = \
            sc.textFile('hdfs://master:9000/project/movies.csv'). \
            map(lambda x : split_complex(x)). \
            filter(lambda x : x if x[3] != "" and x[5] != '0' and x[6] != '0' and int(x[3].split('-')[0]) >= 2000 else None). \
            map(lambda x : (x[3].split('-')[0], x)). \
            reduceByKey(lambda x1, x2 : max(x1, x2, key=lambda x : ((float(x[6])-float(x[5]))/float(x[5]))*100)). \
            map(lambda x : (x[0], x[1][1], round(((float(x[1][6])-float(x[1][5]))/float(x[1][5])), 2))). \
            coalesce(1, True). \
            sortBy(lambda x : x[0])

    res.saveAsTextFile('hdfs://master:9000/project/output/query1-rdd')

    t2 = time.time()
    print("Time:",t2 - t1,"secs")

    return t2 - t1

if __name__ == "__main__":
    q1_rdd()

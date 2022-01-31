from pyspark.sql import SparkSession
import time

spark = SparkSessiON.builder.appName('query4-sql').getOrCreate()

df = spark.read.format('csv').optiONs(header='false', inferSchema='true')

df1 = df.load('hdfs://master:9000/project/movies.csv')
df2 = df.load('hdfs://master:9000/project/movie_genres.csv')

df1.registerTempTable("movies")
df2.registerTempTable("genres")

def q4_sql():

    sqlString = \
        "SELECT (YEAR(movies._c3) DIV 5)*5 AS period, AVG(LENGTH(movies._c2) - " + \
        "LENGTH(REPLACE(movies._c2, ' ', ''))+1) AS mean" + \
        "FROM movies" + \
        "INNER JOIN genres ON movies._c0 = genres._c0" + \
        "WHERE YEAR(movies._c3)>2000 AND genres._c1 = 'Drama'" + \
        "GROUP BY YEAR(movies._c3) DIV 5" + \
        "ORDER BY YEAR(movies._c3) DIV 5 ASC"

    t1 = time.time()

    res = spark.sql(sqlString).coalesce(1).write.json('hdfs://mASter:9000/project/output/query4-sql')

    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q4_sql()

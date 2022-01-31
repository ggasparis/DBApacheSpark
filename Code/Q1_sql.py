from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName('query1-sql').getOrCreate()

df = spark.read.format('csv').options(header='false', inferSchema='true')

df1 = df.load('hdfs://master:9000/project/movies.csv')
df1.registerTempTable('movies')

def q1_sql():

    sqlString = \

        "SELECT Year(_c3) AS year, (_c1) AS Title, ((_c6-_c5)/(_c5))*100 AS profit"+ \
        "FROM movies" + \
        "INNER JOIN ( SELECT Year(_c3) AS year, MAX(((_c6-_c5)/(_c5))*100) AS profit" + \
        "FROM movies" + \
        "WHERE (_c3) is NOT  NULL AND YEAR(_c3) >= 2000 AND (_c5) <> 0 AND (_c6) <> 0" + \
        "GROUP BY  YEAR(_C3)) MAXPROFIT ON MAXPROFIT.year = Year(movies._c3) AND" + \
        "MAXPROFIT.profit = (((movies._c6-movies._c5)/movies._c5)*100) ORDER BY year DESC"

    t1 = time.time()

    res = spark.sql(sqlString).coalesce(1).write.json('hdfs://master:9000/project/output/query3-sql')

    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q3_sql()

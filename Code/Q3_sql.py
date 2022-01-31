FROM pyspark.sql import SparkSessiON
import time

spark = SparkSessiON.builder.appName('query3-sql').getOrCreate()

df = spark.read.format('csv').optiONs(header='false', inferSchema='true')

df1 = df.load('hdfs://mASter:9000/project/ratings.csv')
df2 = df.load('hdfs://mASter:9000/project/movie_genres.csv')

df1.registerTempTable("ratings")
df2.registerTempTable("genres")

def q3_sql():

    sqlString = \
        "SELECT genres._c1 AS Genre , AVG(t2.Rating) AS Rating, COUNT( DISTINCT genres._c0) AS NoMovies" + \
        "FROM genres" + \
        "INNER JOIN (SELECT _c1 AS id_movie,AVG(_c2) AS Rating" + \
        "FROM ratings GROUP BY _c1) t2 ON genres._c0=t2.id_movie" + \
        "GROUP BY genres._c1"

    t1 = time.time()

    res = spark.sql(sqlString).coalesce(1).write.json('hdfs://mASter:9000/project/output/query3-sql')

    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q3_sql()

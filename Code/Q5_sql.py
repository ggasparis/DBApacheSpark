from pyspark.sql import SparkSessiON
import time

spark = SparkSessiON.builder.appName('query5-sql').getOrCreate()

df = spark.read.format('csv').options(header='false', inferSchema='true')

df1 = df.load('hdfs://mASter:9000/project/ratings.csv')
df2 = df.load('hdfs://mASter:9000/project/movie_genres.csv')

df1.registerTempTable("ratings")
df2.registerTempTable("genres")

def q5_sql():

    sqlString = \
        "SELECT temp_1.genre AS genre , MAX(temp_0.user_id) AS user_id, MAX(temp_1.maxnum) AS reviews"+ \
        "FROM (SELECT ratings._c0 AS user_id, genres._c1 AS genre, count(*) AS number"+ \
            "FROM  ratings INNER JOIN genres"+ \
            "ON ratings._c1 = genres._c0" + \
            "GROUP BY ratings._c0, genres._c1)" + \
        "temp_0 INNER JOIN (SELECT temp.genre AS genre, MAX(temp.number) AS maxnum" + \
            "FROM (SELECT ratings._c0 AS user_id, genres._c1 AS genre, count(*) AS number" + \
                "FROM ratings INNER JOIN genres" + \
                "ON ratings._c1 = genres._c0" + \
                "GROUP BY ratings._c0, genres._c1)" + \
            "temp GROUP BY temp.genre"+ \
            "ORDER BY temp.genre ASC) temp_1" + \
        "ON temp_0.genre = temp_1.genre" + \
        "AND temp_0.number = temp_1.maxnum" + \
        "GROUP BY temp_1.genre"+ \
        "ORDER BY temp_1.genre ASC"

    t1 = time.time()

    res = spark.sql(sqlString).coalesce(1).write.jsON('hdfs://mASter:9000/project/output/query5-sql')

    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q5_sql()

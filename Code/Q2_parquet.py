from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName('query2-parquet').getOrCreate()

df = spark.read.format('parquet')

ratings = df.load('hdfs://master:9000/project/ratings.parquet')
ratings.registerTempTable('ratings')

def q2_sql():
    t1 = time.time()
    sqlString = \
        "SELECT (y.no_users*100)/x.all_users AS percentage" + \
        "FROM (SELECT count(DISTINCT ratings._c0) AS all_users FROM ratings) x"+ \
        "CROSS JOIN (SELECT count(*) AS no_users" + \
        "FROM (SELECT (_c0) AS id_users" + \
        "AVG(_c2) AS total" + \
        "FROM ratings GOUP BY (_c0) ORDER BY (_c0) ASc)"+ \
        "AS rat WHERE rat.total > 3.0 ) y"

    res = spark.sql(sqlString).coalesce(1).write.json('hdfs://mASter:9000/project/output/query2-parquet')
    t2 = time.time()

    print("Time:",t2 - t1,"secs")
    return t2 - t1

if __name__ == "__main__":
    q2_sql()

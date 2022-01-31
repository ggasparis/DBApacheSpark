from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CSV2Parquet').getOrCreate()
#if __name__ == "__main__":
#       sc = SparkContext(appName="CSV2Parquet")
#       sqlContext = SQLContext(sc)

# movies to parquet
df_movies = spark.read.csv('hdfs://master:9000/project/movies.csv')
df_movies.write.parquet('hdfs://master:9000/project/movies.parquet')

#  movie_genres to parquet
df_movieG = spark.read.csv('hdfs://master:9000/project/movie_genres.csv')
df_movieG.write.parquet('hdfs://master:9000/project/movie_genres.parquet')

# ratings to paquet
df_ratings = spark.read.csv('hdfs://master:9000/project/ratings.csv')
df_ratings.write.parquet('hdfs://master:9000/project/ratings.parquet')

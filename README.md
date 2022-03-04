# DBApacheSpark

Assignment as it’s given from the course of [dblab](http://web.dbnet.ntua.gr/en/home.html)

### Using Apache Spark in Databases

In this work, we will use Apache Spark to compute detailed queries on files that describe datasets. Apache Spark offers two basic APIs for implementing queries:

* RDD API

✓ https://spark.apache.org/docs/2.4.4/rdd-programming-guide.html
* Dataframe API / Spark SQL

✓ https://spark.apache.org/docs/2.4.4/sql-programming-guide.html

Using the second API in the work recommends implementing with traditional SQL as described in the section https://spark.apache.org/docs/2.4.4 / sql-programming- guide.html # running-sql-queries-programmatically
For the purposes of this work, a set of movie data from the Full MovieLens Dataset set can be used, which can be found here. In this work we will use a subset of a version available on [Kaggle](https://www.kaggle.com/rounakbanik/the-movies-dataset), which you can download via the following link:

   http://www.cslab.ntua.gr/courses/atds/movie_data.tar.gz

In the compressed file you downloaded , contains three text files in CSV format, movie_genres.csv, movies.csv and ratings.csv with a total size of 700MB after decompression. For each file, we then give its first two rows and a brief explanation of each column that exists after two CSV separators (",").

* movies.csv file (17 MB)
 
The movies.csv file describes the movies in the dataset. The first, second and third fields are the identifier, the title and the summary of the film. The fourth field gives the release date (as a timestamp eg 1995-10-30T00: 00: 00.000 + 02: 00), while the fifth gives the duration of the film in minutes. The sixth and seventh fields are the production costs and the income from the screening of the film respectively. The last and eighth fields describe the film's popularity. The file consists of about 45K records.

E.g.
| 862, Toy Story, Led by Woody Andys toys live happily in his room until Andys birthday brings Buzz Lightyear onto the scene Afraid of losing his place in Andys heart Woody plots against Buzz But when circumstances separate Buzz and Woody from their owner the duo eventually learns to put aside their differences, 1995-10-30T00: 00: 00.000 + 02: 00,81.0,30000000,373554033,21.946943 |
| ------- |
8844, Jumanji, When siblings Judy and Peter discover an enchanted board game that opens the door to a magical world they unwittingly invite Alan an adult whos been trapped inside the game for 26 years into their living room Alans only hope for freedom is to finish the game which proves risky as all three find themselves running from giant rhinoceroses evil monkeys and other terrifying creatures, 1995-12 -15T00: 00: 00.000 + 02: 00,104.0,65000000,262797249,17.015539 |

* movie_genres.csv file (1.3 MB)

The movie_genres.csv file contains in each line a pair of movie ID and movie category in the first k the second place respectively. A movie can belong to more than one category and so for each movie there may be more than one line. The file consists of approximately 91K records.

E.g.
| 862, Animation 
| -------- |
| 862, Comedy |
| 862, Family |
| 8844, Adventure |
| 8844, Fantasy |
| 8844, Family |

* The ratings.csv file (677 MB)

The ratings.csv file describes the user ratings for the movies, ie that user x (field1) evaluate the film y (field 2ο) with a score of z (field 3ο) at some point in time (field 4ο). The file consists of about 26M records.

E.g.
| 1,110,1.0,1425941529 |
| -------- |
| 1,147,4.5,1425942435 |
| 1,858,5.0,1425941523 |
| 1,1221,5.0,1425941546 |

#### Part 1

Calculating Detailed Questions with the Apache Spark APIs
In the first part of the work the results for 5 questions of interest will be calculated from the available data set. The questions are presented in Table Table 1.

**Table 1**. Queries for the 1st part

| Query  | Verbal description |
| ------------- | :-----|
| Q1  | From 2000 onwards, to find the highest-grossing film for each year, Ignore entries that have no release date or zero revenue or budget. |
| Q2  |   Find the percentage of users (%) who have given movies an average rating greater than 3.|
| Q3  |   For each movie genre, find the average rating of the genre and the number of movies that fall into that genre. If a film corresponds to more than one genre, we consider it to be measurable in each genre.|
| Q4 | For “Drama” films, find the average length of film summary every 5 years from 2000 onwards (1st2005-2009 5 years: 2000-2004, 2nd2010 , 3rd2015 - 2014, 4th- 2019 ).|
| Q5 |For each type of movie, find the user with the most reviews, along with their most and least favorite movie according to their ratings. In case the user has the same highest / lowest rating in more than 1 movies, select the most popular movie from those in the category that coincides with the user's best / operator rating. The results should be in alphabetical order of the movie category and presented in a table with the following columns.(type, with more reviews, number of reviews,more favorite movie, rating more favorite movie, less favorite movie, rating less favorite movie) 

Additional clarifications and help with questions are given in the Suggestions in this section. The requirements of the first part are the following:

**Request 1** : Upload the 3 CSV files given to you in hdfs. 

**Request 2** : As mentioned your data is given in plain text (csv) format. However, it is known that calculating analytical queries directly on csv files is not efficient. To optimize data access, databases traditionally load data into specially designed binary formats. Although Spark is not a standard database, but a distributed processing system, for performance reasons, it supports a similar logic. Instead of running our queries directly on the csv files, we can first convert the dataset to a special format that:

* Has a smaller footprint in memory and disk and therefore optimizes I / O (input / output) reducing execution time.
* Maintains additional information, such as statistics on the dataset, which help to process it more efficiently. For example, if I search in a data set the values ​​that are greater than 100 and in each block of the dataset I have information about what is the min and what is the max value, then I can bypass the processing of blocks with a max value <100 saving so processing time.

The special format we use to achieve the above is Apache Parquet. When we load a panel into a Parquet, it is converted and stored in a columnar format that optimizes I / O and memory usage and has the features we mentioned. More information about Parquet can be found [here](https://parquet.apache.org). In terms of code, converting a dataset to Parquet is very simple. Examples and information on how to read and write Parquet files can be found [here](https://spark.apache.org/docs/2.4.4/sql-programming-guide.html#parquet-files).

In this query, you are asked to convert any csv in hdfs to Parquet format by reading each CSV in dataframe and then saving it in parquet format back to hdfs (see also the instructions above). Finally there should be 6 files in hdfs, 3 CSV and 3 parquet.

**Rquest 3** :  For each query in Table 1, implement one solution with the RDD API and one with Spark SQL, which can read either CSV files using the inferSchema option or Parquet files. 

**Request 4** : Execute the implementations of request 3 for each query.and runtime from the following 3 cases:
* Map Reduce Queries - RDD API
* SparkSQLinput archive (includeinferchechema)
* Spark SQL by entering the parquet file

**Tips & Warnings**

1. To read the movies file, pay attention to the RDD API as there is a "," separator character in some movie titles. Purely for this case then a function breaks one line of this file into Python language.

        from io import StringIO import csv
        def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=',')) [0]

2.  As profit in Q1 you consider

![alt text](https://github.com/ggasparis/DBApacheSpark/blob/main/images/image1.png)

3.  As average rating of the item in Q3, we consider the average value of the average rating of each movie, according to user ratings, ie:

![alt text](https://github.com/ggasparis/DBApacheSpark/blob/main/images/image2.png)

Indicatively, to verify the solution you are given the answer line of the question for the category "Action"
		(Action, 3.1585329294510713, 1175)

4.  To find the 5 years to which a movie belongs and to find the length of the summary of a movie you can use udfs for the case of sql Q4 questions.

5.  Indicatively, to verify your solution in Q5 is given the answer line of the question for the category "Action"
('Action', 8659, 588, 'The Bourne Identity', 4.0, 'GI Jane', 2.0)


**Part 2**

Implementation and study of integration in queries and Study of the Spark optimizer
In the second topic you are invited to study and evaluate the different implementations that exist in the Map-Reduce environment of Spark for data integration (join) and specifically the repartition join (aka Reduce-Side join) (Paragraph 3.1 and pseudocode A.1 of the publication) and broadcast join (aka Map-Side join) (Paragraph 3.2 and pseudocode A.4) as described in “A Comparison of Join Algorithms for Log Processing in MapReduce” , Blanas et al, in Sigmod 2010”. Broadcast join is considered more efficient in case of joining a large fact table and a relatively smaller dimension table.

**Request 1**: Implement the broadcast join in the RDD API (Map Reduce)

**Request 2** : Implement the repartition join in the RDD API (Map Reduce)

**Request 3** : Isolate 100 rows of the movie genres table in another CSV. Compare the execution times of your two implementations for joining the 100 lines with the ratings table and compare the results. What is observed? Why?
Requirement 4: SparkSQL has both types of merging queries implemented in the DataFrame API. Specifically, based on the structure of the data and calculations we want as well as the user settings, it performs some optimizations in the execution of the query by itself using a query optimizer, something that all databases have. Such an optimization is that it automatically selects the implementation to use for a join query taking into account the size of the data and often changes the order of some operators in an attempt to reduce the total query execution time. If one table is small enough (based on a limit set by the user) it will use the broadcast join, otherwise it will make a repartition join. More information on SparkSQL optimization settings can be found here.

Using the script on the next page, complete <> so that you can disable the join option from the optimizer. Run the query with and without an optimizer and present the results in the form of a bar graph and the execution plan generated by the optimizer in each case. What do you notice? Explain.

    from pyspark.sql import SparkSession import sys, time
    disabled = sys.argv[1]
    spark = SparkSession.builder.appName('query1-sql').getOrCreate()
    if disabled == "Y":
    spark.conf.set(<field name>, <value>)
    elif disabled == 'N': pass
    else:
    df = spark.read.format("parquet")
    raise Exception ("This setting is not available.") df1 = df.load(<path for the ratings table on hdfs>)
    df2 = df.load(<path for the movie_genres table on hdfs>) df1.registerTempTable("ratings")
    df2.registerTempTable("movie_genres") sqlString = \
    "SELECT * " + \ "FROM " + \
    "  (SELECT * FROM movie_genres LIMIT 100) as g, " + \
    "  ratings as r " + \ "WHERE " + \
    "       r._c1 = g._c0"

    t1 = time.time() spark.sql(sqlString).show() t2 = time.time()
    spark.sql(sqlString).explain()
    print("Time with choosing join type %s is %.4f sec."%("enabled" if
    disabled == 'N' else "disabled", t2-t1))











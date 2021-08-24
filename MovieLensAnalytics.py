# Databricks notebook source
# spark - SparkSession
# DataFrame - Structured Data (Meta Data like column name, column data type known as Schema) + Data Rows (rows/data)
# DataFrame is an API, underlying engine is Spark SQL
 
# define the schema ourself, better approach
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType

movieSchema = StructType()\
              .add("movieId", IntegerType(), True)\
              .add("title", StringType(), True)\
              .add("genres", StringType(), True)


movieDf = spark.read.format("csv")\
         .option("header", True)\
         .schema(movieSchema) \
         .load('/mnt/movielens/movies') # we give until the folder, it can load all the movies files in the same folder

movieDf.printSchema()
movieDf.show(4)

# COMMAND ----------

# ratingDf usign schema
# how to avoid using \ for new line continuation with paranthesis

ratingSchema = (StructType()
         .add("userId", IntegerType(), True)
         .add("movieId", IntegerType(), True)
         .add("rating", DoubleType(), True)
         .add("timestamp", LongType(), True))


ratingDf = (spark.read.format("csv")
         .option("header", True)
         .schema(ratingSchema)
         .load('/mnt/movielens/ratings'))
 
ratingDf.printSchema()
ratingDf.show(4)

# COMMAND ----------

# DF are immutable
# any transformation applied created new data frame
# ever data frame has rdd df.rdd
# we need spark session to work with dataframe
# in any spark application, THERE MUST BE 1 spark context, and as many spark sessions

# COMMAND ----------

df = ratingDf.select("movieId", "userId", "rating")
df.printSchema()
df.show(2)

# COMMAND ----------

# create/derive a column from existing one
ratingDf.withColumn("rating_adjusted", ratingDf.rating + 0.2).show(2)

# COMMAND ----------

# rename the column
ratingDf.withColumnRenamed("rating", "ratings").show(2)

# COMMAND ----------

ratingDf.select('*').show(2)

# COMMAND ----------

# derived column from select expression, use alias to rename it
ratingDf.select(ratingDf.rating, (ratingDf.rating + 0.2).alias("adjusted") ).show(2)

# COMMAND ----------

# filter and where are same, no difference 
ratingDf.filter (ratingDf.rating >= 4.5).show(2)

ratingDf.where (ratingDf.rating >= 4.5).show(2)

# COMMAND ----------

# and condition

ratingDf.filter( (ratingDf.rating > 3)  & (ratingDf.rating <= 4) ).show(3)

# COMMAND ----------

# col - column, a function that returns col object
from pyspark.sql.functions import col # 151

print ("Col ", col("rating"))
print ("Col", ratingDf.rating)
# to know all columns
print ("Columns ", ratingDf.columns)

# COMMAND ----------

# ratingDf.filter ("rating" > 4.0).show(2) # error, use col or ratingDf.rating
ratingDf.filter( col("rating") > 4.0).show(2)

# COMMAND ----------

from pyspark.sql.functions import asc, desc

# by default it is ascending order
ratingDf.sort("rating").show(3)
ratingDf.sort(asc("rating")).show(3)

# COMMAND ----------

ratingDf.sort(desc("rating")).show(5)

# COMMAND ----------

from pyspark.sql.functions import upper,lower

movieDf.show(2)
movieDf.select( upper(col( 'title')), lower( col( 'genres'))).show(2)

# COMMAND ----------

# import all the functions with alias name F
import  pyspark.sql.functions as F
movieDf.select( F.upper(F.col( 'title')), F.lower( F.col( 'genres'))).show(2)

# COMMAND ----------

# groupBy, aggregations
# analytics, to find most popular movies based avg  rating > 3.5, total numbers of user rated the movie >= 100
# sort by avg rating

mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(F.count("userId"), F.avg("rating").alias("avg_rating"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .filter( (F.col("total_ratings") >= 100) & (F.col("avg_rating") >= 3.5))\
                .sort(F.desc("avg_rating"))

mostPopularDf.printSchema()
mostPopularDf.count()
# mostPopularDf.show(10)

# COMMAND ----------

movieDf.show(2)

# COMMAND ----------

mostPopularMoviesDf = mostPopularDf\
                      .join(movieDf, movieDf.movieId == mostPopularDf.movieId)\
                      .select(mostPopularDf.movieId, 'title', 'total_ratings', 'avg_rating', 'genres')

mostPopularMoviesDf.show(5)

# COMMAND ----------

# we have result, now write back to data lake

#write result as is

# it will create a folder, not a file, the files shall be named as with part-00xxxxx - means parition id
mostPopularMoviesDf.write.mode('overwrite')\
                   .csv('/mnt/movielens/analytics/most-popular-movies')


# COMMAND ----------

# reduce the number of partitions to 1 , generate single file

# reduce to single partition and write to file system
mostPopularMoviesDf.coalesce(1)\
                   .write\
                   .option('header', True)\
                   .mode('overwrite')\
                   .csv('/mnt/movielens/analytics/most-popular-movies-single')


# COMMAND ----------



# Databricks notebook source
# the SparkContext and SparkSession are aalready initialized in notebook
sc # SparkContext
# spark session - entry point for DF/SQL
spark

# COMMAND ----------

# read a file from DataBricks
# lazy evaluation, file is not read here, instead it will be read later when action executed
# CREATE RDD from file
fileRdd = sc.textFile("/FileStore/tables/wordcount/shakespeare.txt") 

# COMMAND ----------

# Action method, execute RDD, THIS IS NOT LAZY, actual execution
# will initiate spark job, read file from the location, load into memory /partition

# count is action method
# it execute the operation and bring back the results to driver/this notebook
fileRdd.count() # how many lines

# COMMAND ----------

# collect is action method, collect result from spark partition
fileRdd.collect()

# COMMAND ----------

# Transformations are LAZY functions, won't executed until an action applied
# data massaging/ETL/Logic/cleaning

nonEmptyLinesRdd = fileRdd.filter ( lambda line: line != "") 
nonEmptyLinesRdd.collect()

# COMMAND ----------

# RDDs are IMMUTABLE, THEY CAN"T CHANGED
# ANY NEW RDD, CHILD RDD created will its own data
lowerCaseRdd = nonEmptyLinesRdd.map (lambda line: line.lower() )
# take method, take fetch first n records from begining of partition
lowerCaseRdd.take(5)

# COMMAND ----------

# split line into list of words per each line
wordsListRdd = lowerCaseRdd.map (lambda line: line.split(" "))
wordsListRdd.take(4)

# COMMAND ----------

# flatten the list , now we have list of list of words INTO list of words
# flatMap, transformation
wordsRdd = wordsListRdd.flatMap(lambda list: list)
wordsRdd.take(10) 

# COMMAND ----------

# make paired RDD means, Key and Value tuple (william, 1)
wordsPairRdd = wordsRdd.map (lambda word:  (word, 1) )
wordsPairRdd.take(5)

# COMMAND ----------

# word count, like using groupBy count SELECT word, count(word) from words group by word
"""
INPUT
(apple, 1)  <-- first time apple word appear, it won't call lambda acc, value: acc + value, but initialize temp data with initial value
(orange, 1)<-- first time orange word appear, it won't call lambda acc, value: acc + value, but initialize temp data with initial value
(banana, 1)<-- first time banana word appear, it won't call lambda acc, value: acc + value, but initialize temp data with initial value
(apple, 1) <-- apple appear second time, now IT WILL CALL LAMBDA lambda acc, value: acc + value, (1, 1): 1 + 1 (2 as ouput), 2 is assignd in acc
(orange, 1)<-- orange appear second time, now IT WILL CALL LAMBDA lambda acc, value: acc + value, (1, 1): 1 + 1 (2 as ouput), 2 is assignd in acc
(apple, 1)<-- apple appear 3rd time, now IT WILL CALL LAMBDA lambda acc, value: acc + value, (2, 1): 2 + 1 (3 as ouput), 3 is assignd in acc

# reduceByKey output
# acc means accumulator, value is from the tuple example,  (apple, 1), where 1 is value
  ============
  word   |  acc
  ============
  apple  |   3
  orange |   2
  banana |   1
"""

wordCountRdd = wordsPairRdd.reduceByKey(lambda acc, value: acc + value)
wordCountRdd.take(100)


# COMMAND ----------


# output will be written to text file
wordCountRdd.saveAsTextFile('/FileStore/tables/wordcount/results-final')


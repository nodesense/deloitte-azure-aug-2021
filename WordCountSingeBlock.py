# Databricks notebook source
# \ means here is new line continueation
wordsCount = sc.textFile("/FileStore/tables/wordcount/shakespeare.txt")\
               .filter ( lambda line: line != "") \
               .map (lambda line: line.lower() )\
               .map (lambda line: line.split(" "))\
               .flatMap(lambda list: list)\
               .map (lambda word:  (word, 1) )\
               .reduceByKey(lambda acc, value: acc + value)

wordsCount.take(10)

# COMMAND ----------

# line continuation without \ but using paranthesis ()
wordsCount = (
                sc.textFile("/FileStore/tables/wordcount/shakespeare.txt")
               .filter ( lambda line: line != "")
               .map (lambda line: line.lower() )
               .map (lambda line: line.split(" "))
               .flatMap(lambda list: list)
               .map (lambda word:  (word, 1) )
               .reduceByKey(lambda acc, value: acc + value) 
             )

wordsCount.take(100)

# COMMAND ----------



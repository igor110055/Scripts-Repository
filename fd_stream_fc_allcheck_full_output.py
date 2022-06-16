import sys
import logging
import json
import pyspark.sql

from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext('local')
spark = SparkSession(sc)
session = SparkSession.builder.getOrCreate()

connectionString = "Endpoint=sb://azredeevthub.servicebus.windows.net/;SharedAccessKeyName=cosmos;SharedAccessKey=IJYh+RNMXlQl50v9dDX8cSAiMwiWyCdYkqnEeYjOxvc=;EntityPath=fc_all_check_analise_full_output"

ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.consumergroup' : "batch"
}

df = spark.readStream.format("eventhubs").options(**ehConf).load()

#df.printSchema()

messages = df.withColumn("body", df["body"].cast("string"))

#messages.printSchema()

messages2 = messages.select("body")

messages2.printSchema()

messages2.writeStream.outputMode("append").format("text").option("path","/data/raw/FICO/AllCheckAnaliseFullOutput/streaming").option("checkpointLocation","/data/raw/FICO/AllCheckAnaliseFullOutput/streaming/checkpoint").start().awaitTermination()

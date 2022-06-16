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

schema = StructType([StructField("cabecalho",StructType([
                                       StructField("cpf", StringType(), True),
                                       StructField("telefone",StringType(), True),
                                       StructField("timestamp", StringType(), True),
                                       StructField("identificador-job", StringType(), True),
                                       StructField("session-id", StringType(), True),
                                       StructField("sistema-origem", StringType(), True),
                                       StructField("tipo-transacao", StringType(), True)])),
                     StructField("clear-sale-dti",StructType([
                                       StructField("ID", StringType(), True),
                                       StructField("Document", StringType(), True),
                                       StructField("BirthDate", StringType(), True),
                                       StructField("ReferenceDate", StringType(), True),
                                       StructField("Score", StringType(), True),
                                       StructField("Date", StringType(), True),
                                       StructField("Digital", StringType(), True)]))
])

connectionString = "Endpoint=sb://azredeevthub.servicebus.windows.net/;SharedAccessKeyName=cosmos;SharedAccessKey=oL8GFHgPCjCaEPrSNTS6acnO+ZQYV9CRbw0v88F0F58=;EntityPath=fc_clear_sale_dti_output"

ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.consumergroup' : "batch"
}

df = spark.readStream.format("eventhubs").options(**ehConf).load()


messages = df.withColumn("body", df["body"].cast("string")).select(from_json("body",schema).alias("body")).select("body.cabecalho.cpf","body.cabecalho.telefone","body.cabecalho.timestamp","body.cabecalho.identificador-job","body.cabecalho.session-id","body.cabecalho.sistema-origem","body.cabecalho.tipo-transacao","body.clear-sale-dti.ID","body.clear-sale-dti.Document","body.clear-sale-dti.BirthDate","body.clear-sale-dti.ReferenceDate","body.clear-sale-dti.Score","body.clear-sale-dti.Date","body.clear-sale-dti.Digital")

messages.writeStream.outputMode("append").format("csv").option("sep", ";").option("path","/data/raw/FICO/ClearSaleDTIOutput/streaming").option("checkpointLocation","/data/raw/FICO/ClearSaleDTIOutput/streaming/checkpoint").start().awaitTermination()


                                                                                                                                                         47,0-1        Bot



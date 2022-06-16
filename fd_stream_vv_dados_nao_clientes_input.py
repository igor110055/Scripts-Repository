#*******************************************************************************************
# NOME: FD_STREAM_VV_DADOS_NAO_CLIENTES_INPUT.PY
# OBJETIVO: CARREGAR AS INFORMACOES QUE SAO GERADAS NA FILA vv_dados_nao_cliente_input DO
#           EVENT HUB E ARMAZENAR AS INFORMACOES NA PASTA STREAMING.
# CONTROLE DE VERSAO
# 30/07/2019 - VERSAO INICIAL - ARIANE TATEISHI
#*******************************************************************************************
import sys
import logging
import json
import pyspark.sql
import subprocess

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
   conf = SparkConf().setAppName("VivereDadosNaoCliente")
   sc = SparkContext(conf=conf)
   spark = SparkSession.builder.appName("VivereDadosNaoCliente").getOrCreate()

#Carrega as variaveis de configuracao
def carregaLib():
    args = "rm -r lib"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    _getLibs = "/data/source/lib"
    cmd = ['hdfs', 'dfs', '-get', _getLibs, '.']
    subprocess.check_output(cmd).strip().split('\n')
    sys.path.insert(0, "lib")
carregaLib()
import Config.Constants as conf

#Define o schema do JSON a ser carregado
schema = StructType([StructField("cabecalho",StructType([
                                       StructField("cpf", StringType(), True),
                                       StructField("telefone",StringType(), True),
                                       StructField("timestamp", StringType(), True),
                                       StructField("identificador-job", StringType(), True),
                                       StructField("session-id", StringType(), True),
                                       StructField("sistema-origem", StringType(), True),
                                       StructField("tipo-transacao", StringType(), True)])),
                     StructField("dados-nao-cliente",StructType([
                                       StructField("data-hora", StringType(), True),
                                       StructField("cpf", StringType(), True),
                                       StructField("email", StringType(), True),
                                       StructField("nome", StringType(), True),
                                       StructField("celular", StringType(), True),
                                       StructField("data-nascimento", StringType(), True)]))
])

#Recupera as variaveis de configuracao
connectionString = conf.getPolicyListner('vv_dados_nao_cliente_input')
consumergroup = conf.getConsumerGroup('vv_dados_nao_cliente_input')

ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.consumergroup' : consumergroup
}

# definicao do data frame a ser gerado
df = spark.readStream.format("eventhubs").options(**ehConf).load()
messages = df.withColumn("body", df["body"].cast("string")).select(from_json("body",schema).alias("body")).select("body.cabecalho.cpf","body.cabecalho.telefone","body.cabecalho.timestamp","body.cabecalho.identificador-job","body.cabecalho.session-id","body.cabecalho.sistema-origem","body.cabecalho.tipo-transacao","body.dados-nao-cliente.data-hora","body.dados-nao-cliente.cpf","body.dados-nao-cliente.email","body.dados-nao-cliente.nome","body.dados-nao-cliente.celular","body.dados-nao-cliente.data-nascimento")

# escrita dos eventos da fila na pasta streaming.
messages.writeStream.outputMode("append").format("csv").option("sep", ";").option("path","/data/raw/Vivere/DadosNaoCliente/streaming").option("checkpointLocation","/data/raw/Vivere/DadosNaoCliente/streaming/checkpoint").start().awaitTermination()

######## -*- coding: utf-8 -*-######

from cassandra.auth import PlainTextAuthProvider
# from prettytable import PrettyTable
from cassandra.cluster import Cluster
from ssl import PROTOCOL_TLSv1_2, CERT_REQUIRED
from requests.utils import DEFAULT_CA_BUNDLE_PATH
# from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
 
if __name__ == "__main__":
   conf = SparkConf().setAppName("teste")
   sc = SparkContext(conf=conf)
   spark = SparkSession.builder.appName("teste").getOrCreate()

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName('log_validacao_carga') \
#         .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
#         .config("hive.exec.dynamic.partition", "true") \
#         .config("hive.exec.dynamic.partition.mode", "nonstrict") \
#         .config("spark.sql.parquet.writeLegacyFormat", "true") \
#         .config("spark.sql.hive.llap", "false") \
#         .enableHiveSupport() \
#         .getOrCreate()

    # Variável global que retorna a data atual.
    # current_date = dt.datetime.now().strftime('%d/%m/%Y')
    # print(current_date)

    # spark2 = SparkSession.builder.getOrCreate()

ssl_opts = {
    'ca_certs': DEFAULT_CA_BUNDLE_PATH,
    'ssl_version': PROTOCOL_TLSv1_2,
    'cert_reqs': CERT_REQUIRED  # Certificates are required and validated
}

auth_provider = PlainTextAuthProvider(username="azredcosmosdb",
                                        password="QufWVWQNvrMFuOZzU7Q7EURNVqX2MLpgq2zG0pRgh8Vy9XyUlDRE70L60TLnZbjUU6UtP22DGzpFKjGw2HYErw==")
cluster = Cluster(["azredcosmosdb.cassandra.cosmos.azure.com"], port=10350, auth_provider=auth_provider, ssl_options=ssl_opts)
session = cluster.connect("fico")

# df = spark2.read.csv('/Users/cristina.cruz/Desktop/teste_amostra2.csv', header=True, sep=';')
df = spark.read.csv('abfs://cosmosdb@azredlake.dfs.core.windows.net/teste_amostra2.csv', header=True, sep=';')
df.show(10, False)

df_pem_pandas = df.toPandas()
print(df_pem_pandas)

query = "INSERT INTO pem6 (cpf,score_cetip,score_serasa_balde,score_spc_balde,score_boavista,rating_cadus,rating_bacen,flag_fraude_hist,flag_func,flag_monoprodutista,"\
            "flag_correntista,tipo_vinculo_cc,flag_possui_ctr_sim,renda,nivel_confianca,flag_pap,flag_cupom,publico,coringa1,coringa2,coringa3,coringa4,coringa5,coringa6,coringa7,coringa8,"\
            "coringa9,coringa10,coringa11,coringa12,coringa13,coringa14,id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
prepared = session.prepare(query)

for index, row in df_pem_pandas.iterrows():
    session.execute(prepared, (row['cpf'],row['score_cetip'],row['score_serasa_balde'],row['score_spc_balde'],row['score_boavista'],row['rating_cadus'],row['rating_bacen'],
                            row['flag_fraude_hist'],row['flag_func'],row['flag_monoprodutista'],row['flag_correntista'],row['tipo_vinculo_cc'],row['flag_possui_ctr_sim'],row['renda'],
                            row['nivel_confianca'],row['flag_pap'],row['flag_cupom'],row['publico'],row['coringa1'],row['coringa2'],row['coringa3'],row['coringa4'],row['coringa5'],
                            row['coringa6'],row['coringa7'],row['coringa8'],row['coringa9'],row['coringa10'],row['coringa11'],row['coringa12'],row['coringa13'],row['coringa14'],row['id']))

cluster.shutdown()

spark.stop()
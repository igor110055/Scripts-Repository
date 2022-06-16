from cassandra.auth import PlainTextAuthProvider
#import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
#from prettytable import PrettyTable
import subprocess
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import pyspark.sql.functions as f
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession

if __name__ == "__main__":
    conf = SparkConf().setAppName("teste")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("teste").getOrCreate()

#Carrega as variaveis de configuracao
def carregaConf():
    args = "rm -r conf_cosmos"
    args = "hdfs dfs -get /data/source/config_cosmos.py config_cosmos.py"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    print(args)
    print(s_output)
    print(s_err)

carregaConf()
import config as cfg

def PrintTable(rows):
    t = ['UserID', 'Name', 'City']
    for r in rows:
        t.append([r.user_id, r.user_name, r.user_bcity])
    print(t)

ssl_opts = {
            'ca_certs': DEFAULT_CA_BUNDLE_PATH,
            'ssl_version': PROTOCOL_TLSv1_2,
            }
print("comecou")
print(cfg.config)
if 'certpath' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['certpath']
print("conexao")
auth_provider = PlainTextAuthProvider(username='azredcosmosdb', password='QufWVWQNvrMFuOZzU7Q7EURNVqX2MLpgq2zG0pRgh8Vy9XyUlDRE70L60TLnZbjUU6UtP22DGzpFKjGw2HYErw==')
cluster = Cluster(['azredcosmosdb.cassandra.cosmos.azure.com'], port = 10350, auth_provider=auth_provider, ssl_options=ssl_opts
)
print("conecta")
#cluster = Cluster([])
session = cluster.connect()
print("cria o keyspace")
session.execute('CREATE KEYSPACE IF NOT EXISTS ficoari WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
session.execute('CREATE TABLE IF NOT EXISTS ficoari.pem (cpf text,score_cetip int,score_serasa_balde int,score_spc_balde int,score_boavista int,rating_cadus text,rating_bacen text,flag_fraude_hist text,flag_func text,flag_monoprodutista text,flag_correntista text,tipo_vinculo_cc text,flag_possui_ctr_sim text,renda text,nivel_confianca text,flag_pap text,flag_cupom text,publico text,coringa1 text,coringa2 text,coringa3 text,coringa4 text,coringa5 text,coringa6 text,coringa7 text,coringa8 text,coringa9 text,coringa10 text,coringa11 text,coringa12 text,coringa13 text,coringa14 text,id text PRIMARY KEY)')

df = spark.read.csv('abfs://cosmosdb@azredlake.dfs.core.windows.net/teste_amostra2.csv', header=True, sep=';')
df.show(10, False)

df_pem_pandas = df.toPandas()
print(df_pem_pandas)

query = "INSERT INTO ficoari.pem (cpf,score_cetip,score_serasa_balde,score_spc_balde,score_boavista,rating_cadus,rating_bacen,flag_fraude_hist,flag_func,flag_monoprodutista, flag_correntista,tipo_vinculo_cc,flag_possui_ctr_sim,renda,nivel_confianca,flag_pap,flag_cupom,publico,coringa1,coringa2,coringa3,coringa4,coringa5,coringa6,coringa7,coringa8,coringa9,coringa10,coringa11,coringa12,coringa13,coringa14,id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
prepared = session.prepare(query)

for index, row in df_pem_pandas.iterrows():
    session = cluster.connect()
    session.execute(prepared, (row['cpf'],row['score_cetip'],row['score_serasa_balde'],row['score_spc_balde'],row['score_boavista'],row['rating_cadus'],row['rating_bacen'], row['flag_fraude_hist'],row['flag_func'],row['flag_monoprodutista'],row['flag_correntista'],row['tipo_vinculo_cc'],row['flag_possui_ctr_sim'],row['renda'],row['nivel_confianca'],row['flag_pap'],row['flag_cupom'],row['publico'],row['coringa1'],row['coringa2'],row['coringa3'],row['coringa4'],row['coringa5'],row['coringa6'],row['coringa7'],row['coringa8'],row['coringa9'],row['coringa10'],row['coringa11'],row['coringa12'],row['coringa13'],row['coringa14'],row['id']))
#insert_data = session.prepare("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
#session.execute(insert_data, [1,'Lybkov','Seattle'])
#session.execute(insert_data, [2,'Doniv','Dubai'])
#session.execute(insert_data, [3,'Keviv','Chennai'])
#session.execute(insert_data, [4,'Ehtevs','Pune'])
#session.execute(insert_data, [5,'Dnivog','Belgaum'])
#session.execute(insert_data, [6,'Ateegk','Narewadi'])
#session.execute(insert_data, [7,'KannabbuS','Yamkanmardi'])

rows = session.execute('SELECT * FROM ficoari.pem')
print(rows)


#cluster.shutdown()
spark.stop()


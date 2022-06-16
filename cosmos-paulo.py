######## -*- coding: utf-8 -*-######
########### Versão - 1.0 ###########
############# Base PEM #############
########### Cristina Cruz ##########
####################################

import sys
import os
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime

import time
import uuid
from six.moves import queue
CONCURRENCY_LEVEL = 32

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from ssl import PROTOCOL_TLSv1_2, CERT_REQUIRED
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder.appName('log_validacao_carga') \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.hive.llap", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    # Variável global que retorna a data atual.
    current_date = dt.datetime.now().strftime('%d/%m/%Y')
    print(current_date)

    class TabelasDadosIO:
        _spark = None

        def __init__(self, job):
            if TabelasDadosIO._spark is None:
                TabelasDadosIO._spark = SparkSession.builder.appName(job) \
                    .config("hive.exec.dynamic.partition", "true") \
                    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                    .config("spark.sql.parquet.writeLegacyFormat", "true") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                    .enableHiveSupport() \
                    .getOrCreate()            

        def baldemanual_serasa(self):
            return TabelasDadosIO._spark.table('harmonized.h_baldemanual_serasa')

        def baldemanual_renda(self):
            return TabelasDadosIO._spark.table('harmonized.h_baldemanual_renda')     

        def baldemanual_boavista(self):
            return TabelasDadosIO._spark.table('harmonized.h_baldemanual_boavista')

        def baldemanual_cetip(self):
            return TabelasDadosIO._spark.table('harmonized.h_baldemanual_cetip')

        def baldemanual_coringa(self):
            return TabelasDadosIO._spark.table('harmonized.h_baldemanual_coringa')          

    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        # Dataframes originais
        _parametros = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._baldemanual_serasa = _dados_io.baldemanual_serasa()
            self._baldemanual_renda = _dados_io.baldemanual_renda()
            self._baldemanual_boavista = _dados_io.baldemanual_boavista()
            self._baldemanual_cetip = _dados_io.baldemanual_cetip()
            self._baldemanual_coringa = _dados_io.baldemanual_coringa()

        def gera_regras(self):
            resultado_serasa = baldemanual_serasa(self._baldemanual_serasa)
            # resultado_serasa.show(1,False)

            resultado_renda = baldemanual_renda(self._baldemanual_renda)
            # resultado_renda.show(1,False)

            resultado_boavista = baldemanual_boavista(self._baldemanual_boavista)
            # resultado_boavista.show(1,False)

            resultado_cetip = baldemanual_cetip(self._baldemanual_cetip)
            # resultado_cetip.show(1,False)

            resultado_coringa = baldemanual_coringa(self._baldemanual_coringa)
            # resultado_coringa.show(1,False)

            resultado_join1 = join_renda_serasa(resultado_renda, resultado_serasa, resultado_boavista, resultado_cetip, resultado_coringa)
            # resultado_join1.show(1,False)

            return resultado_join1

    # Função que retorna os dados do arquivo de Balde Manual - Serasa.
    def baldemanual_serasa(df_serasa):
        return (df_serasa)                

    # Função que retorna os dados do arquivo de Balde Manual - Renda.
    def baldemanual_renda(df_renda):
        return (df_renda)

    # Função que retorna os dados do arquivo de Balde Manual - Boa Vista.
    def baldemanual_boavista(df_boavista):
        return (df_boavista)

    # Função que retorna os dados do arquivo de Balde Manual - Cetip.
    def baldemanual_cetip(df_cetip):
        return (df_cetip)               

    # Função que retorna os dados do arquivo de Balde Manual - Coringa.
    def baldemanual_coringa(df_coringa):
        return (df_coringa)
    
    # Join entre as tabelas do Balde Manual.
    def join_renda_serasa(df_renda, df_serasa, df_boavista, df_cetip, df_coringa):
        df = (df_renda
              .join(df_serasa, df_renda['no_cpf']==df_serasa['no_cpf'], 'left')
              .join(df_boavista, df_renda['no_cpf']==df_boavista['no_cpf'], 'left')
              .join(df_cetip, df_renda['no_cpf'] == df_cetip['no_cpf'], 'left')
              .join(df_coringa, df_renda['no_cpf'] == df_coringa['coluna1'], 'left')
              .select(df_renda['no_cpf'].cast('string').alias('cpf'),
                        f.when(df_serasa['cd_score'] == None, 0).otherwise(df_serasa['cd_score']).cast('integer').alias('score_serasa_balde'),
                        df_boavista['cd_score'].cast('integer').alias('score_spc_balde'),
                        df_renda['cd_renda'].cast('string').alias('renda'),
                        df_renda['cd_confianca'].cast('string').alias('nivel_confianca'),
                        df_coringa['coluna2'].cast('string').alias('coringa1'),
                        df_coringa['coluna3'].cast('string').alias('coringa2'),
                        df_coringa['coluna4'].cast('string').alias('coringa3'),
                        df_coringa['coluna5'].cast('string').alias('coringa4'),
                        df_coringa['coluna6'].cast('string').alias('coringa5'),
                        df_coringa['coluna7'].cast('string').alias('coringa6'),
                        df_coringa['coluna8'].cast('string').alias('coringa7'),
                        df_coringa['coluna9'].cast('string').alias('coringa8'),
                        df_coringa['coluna10'].cast('string').alias('coringa9'),
                        df_coringa['coluna11'].cast('string').alias('coringa10'),
                        df_coringa['coluna12'].cast('string').alias('coringa11'),
                        df_coringa['coluna13'].cast('string').alias('coringa12'),
                        df_coringa['coluna14'].cast('string').alias('coringa13'),
                        df_coringa['coluna15'].cast('string').alias('coringa14')
              )
              .withColumn('score_cetip', f.lit(None).cast('integer'))
              .withColumn('score_boavista', f.lit(None).cast('integer'))
              .withColumn('rating_cadus', f.lit(None).cast('string'))
              .withColumn('rating_bacen', f.lit(None).cast('string'))
              .withColumn('flag_fraude_hist', f.lit(None).cast('string'))
              .withColumn('flag_func', f.lit(None).cast('string'))
              .withColumn('flag_monoprodutista', f.lit(None).cast('string'))
              .withColumn('flag_correntista', f.lit(None).cast('string'))
              .withColumn('tipo_vinculo_cc', f.lit(None).cast('string'))
              .withColumn('flag_possui_ctr_sim', f.lit(None).cast('string'))
              .withColumn('flag_pap', f.lit(None).cast('string'))
              .withColumn('flag_cupom', f.lit(None).cast('string'))
              .withColumn('publico', f.lit(None).cast('string'))
              .withColumn('id', f.monotonically_increasing_id().cast('string'))
              )
        return df

    _dados_io = TabelasDadosIO('RED_PEM')

    _gerenciador = Gerenciador(_dados_io)
    df_pem = _gerenciador.gera_regras()
    # df_pem.show(1,False)
    # df_pem.printSchema()
    
    df_pem_pandas = df_pem.toPandas()
    # print(df_pem_pandas)
    df_pem_pandas['score_serasa_balde'] = df_pem_pandas['score_serasa_balde'].fillna(0).astype(int)
    # print(df_pem_pandas)

    ssl_opts = {
        'ca_certs': DEFAULT_CA_BUNDLE_PATH,
        'ssl_version': PROTOCOL_TLSv1_2
        # 'cert_reqs': CERT_REQUIRED  # Certificates are required and validated
    }

    auth_provider = PlainTextAuthProvider(username="azredcosmosdb",
                                          password="QufWVWQNvrMFuOZzU7Q7EURNVqX2MLpgq2zG0pRgh8Vy9XyUlDRE70L60TLnZbjUU6UtP22DGzpFKjGw2HYErw==")
    cluster = Cluster(["azredcosmosdb.cassandra.cosmos.azure.com"], port=10350, auth_provider=auth_provider,
                      ssl_options=ssl_opts)
    session = cluster.connect("fico")

    # auth_provider = PlainTextAuthProvider(username="azredcosmosdb",
    #                                       password="QufWVWQNvrMFuOZzU7Q7EURNVqX2MLpgq2zG0pRgh8Vy9XyUlDRE70L60TLnZbjUU6UtP22DGzpFKjGw2HYErw==")
    # cluster = Cluster(["azredcosmosdb.cassandra.cosmos.azure.com"], port=10350, auth_provider=auth_provider)
    # session = cluster.connect("fico")    

    query = "INSERT INTO pem3 (cpf,score_cetip,score_serasa_balde,score_spc_balde,score_boavista,rating_cadus,rating_bacen,flag_fraude_hist,flag_func,flag_monoprodutista,"\
            "flag_correntista,tipo_vinculo_cc,flag_possui_ctr_sim,renda,nivel_confianca,flag_pap,flag_cupom,publico,coringa1,coringa2,coringa3,coringa4,coringa5,coringa6,coringa7,coringa8,"\
            "coringa9,coringa10,coringa11,coringa12,coringa13,coringa14,id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    prepared = session.prepare(query)


    def clear_queue():
        while True:
            try:
                futures.get_nowait().result()
            except queue.Empty:
                break


    #start = time.time()
    futures = queue.Queue(maxsize=CONCURRENCY_LEVEL)



    for index, row in df_pem_pandas.iterrows():
        future = session.execute_async(prepared, (row['cpf'],row['score_cetip'],row['score_serasa_balde'],row['score_spc_balde'],row['score_boavista'],row['rating_cadus'],row['rating_bacen'],
                                row['flag_fraude_hist'],row['flag_func'],row['flag_monoprodutista'],row['flag_correntista'],row['tipo_vinculo_cc'],row['flag_possui_ctr_sim'],row['renda'],
                                row['nivel_confianca'],row['flag_pap'],row['flag_cupom'],row['publico'],row['coringa1'],row['coringa2'],row['coringa3'],row['coringa4'],row['coringa5'],
                                row['coringa6'],row['coringa7'],row['coringa8'],row['coringa9'],row['coringa10'],row['coringa11'],row['coringa12'],row['coringa13'],row['coringa14'],row['id']))    
        try:
            futures.put_nowait(future)
        except queue.Full:
            clear_queue()
            futures.put_nowait(future)

    clear_queue()
    #end = time.time()

    #print("Finished executing {} queries with a concurrency level of {} in {:.2f} seconds.".format(TOTAL_QUERIES, CONCURRENCY_LEVEL, (end-start)))

    cluster.shutdown()

    spark.stop()
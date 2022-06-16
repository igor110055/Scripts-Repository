
######## -*- coding: utf-8 -*-#########
########### Versão - 1.0 ###########
########### Balde Manual ##########
######## Alex Fernando dos S Silva ####
################################

import sys
import os
from pyspark.sql import SparkSession, HiveContext, SQLContext
from pyspark.sql import types as t
from pyspark import SparkContext
from py4j.java_gateway import java_import
import sys
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as sf
import pandas as pd

caminho_origem = str(sys.argv[1])
nm_processo = str(sys.argv[2])
nm_objeto = str(sys.argv[3])
dt_verificacao = str(sys.argv[4])
formato = str(sys.argv[5])
header = str(sys.argv[6])

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("log_cont_registros") \
        .getOrCreate()

    # Variável global que retorna a data atual.
    current_date = dt.datetime.now().strftime('%d/%m/%Y')
    print(current_date)


    class ArquivosDadosIO:
        _spark = None
        _fs = None

        def __init__(self, job):
            if ArquivosDadosIO._spark is None:
                ArquivosDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()
                java_import(ArquivosDadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')
                ArquivosDadosIO._fs = ArquivosDadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem. \
                get(ArquivosDadosIO._spark._jsc.hadoopConfiguration())

        def spark_session(self):
            return ArquivosDadosIO._spark

        def get_data(self):            
            if formato == 'csv':                
                df = ArquivosDadosIO._spark.read.csv('{}/{}/'.format(caminho_origem, dt_verificacao), header=header, sep=';')
            elif formato == 'txt':
                df = ArquivosDadosIO._spark.read.text('{}/{}/'.format(caminho_origem, dt_verificacao))
                print('{}/{}/'.format(caminho_origem, dt_verificacao))
            return df

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

        def gravar_parquet(self, df, nome):
            df.write.option("encoding", "UTF-8").mode("append").format("parquet").option("header","false").partitionBy('aa_carga').saveAsTable(nome)


    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._spark_session = _dados_io.spark_session()
            self._fonte = _dados_io.get_data()

        def gera_regras(self):
            resultado = cont_registros(self._fonte)

            return resultado


        # Função que retorna os dados do arquivo de Clientes e realiza o tratamento para quebra de colunas.
    def cont_registros(df_fonte):
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)
        try:
            cd_sequencial = sqlContext.sql('select max(cd_sequencial) as max from analytical.a_estrutural_log_validacao_carga_prevista where nm_processo="{}" and nm_objeto="{}" and dt_execucao={}'.format(nm_processo, nm_objeto, dt_verificacao))       
            cd_sequencial = cd_sequencial.collect()[0]['max']
        except:
            cd_sequencial=None
        if cd_sequencial == None:                
            cd_sequencial=1
        else:
            cd_sequencial=int(cd_sequencial)
            cd_sequencial = cd_sequencial + 1

        df_fonte = df_fonte.withColumn('um', sf.lit(1))
        try:
            qt = df_fonte.groupBy('um').sum().collect()[0][1]
        except:
            qt=0
        carregar = [(cd_sequencial, nm_processo, nm_objeto, qt, dt_verificacao, dt_verificacao[0:4])]
        print(qt)
        df = spark.createDataFrame(carregar, ['cd_sequencial', 'nm_processo', 'nm_objeto', 'qt_registros_previstos', 'dt_execucao', 'aa_carga'])
        return df

    _dados_io = ArquivosDadosIO('RED_FINANCEIRA')
    _dados_io2 = TabelasDadosIO('RED_FINANCEIRA')

    _gerenciador = Gerenciador(_dados_io)
    df = _gerenciador.gera_regras()

    tabela_hive = 'analytical.a_estrutural_log_validacao_carga_prevista'
    _dados_io2.gravar_parquet(df, tabela_hive)

    spark.stop()

#    spark.stop()
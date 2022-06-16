######## -*- coding: utf-8 -*-########
# *******************************************************************************************
# NOME: INSERE_QTD_REGISTROS.PY
# OBJETIVO: REALIZA TRATAMENTO DE LOG DE REGISTROS.
# CONTROLE DE VERSAO
# 20/05/2019 - ALEX SILVA - VERSAO INICIAL
# *******************************************************************************************

import sys
import os
from itertools import chain
import subprocess
import traceback
from pyspark.sql import SparkSession, HiveContext, SQLContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark import SparkContext
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as sf


def carregaLib():
    _getLibs = "/data/source/lib"
    cmd = ['hdfs', 'dfs', '-get', _getLibs, '.']
    subprocess.check_output(cmd).strip().split('\n')
    sys.path.insert(0, "lib")


carregaLib()
import Constants.Variables as var
import Constants.Mensage   as msg
import Constants.Path     as pth
import Dados.DadosIO       as db
import Model.fin_proposta  as model
import Util.Function       as ut_f
import Config.Constants    as conf

nm_processo = str(sys.argv[1])
nm_objeto = str(sys.argv[2])
dt_verificacao = str(sys.argv[3])


class DadosIO:
    _spark = None
    _fs = None

    def __init__(self, job):
        if DadosIO._spark is None:
            DadosIO._spark = SparkSession.builder.appName(job) \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .config("spark.sql.parquet.writeLegacyFormat", "true") \
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                .config("spark.sql.hive.llap", "false") \
                .enableHiveSupport() \
                .getOrCreate()
            java_import(DadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')
            DadosIO._fs = DadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem. \
                get(DadosIO._spark._jsc.hadoopConfiguration())

    def spark_session(self):
        return DadosIO._spark

    def gravar_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8").format("parquet").option("header", "false").insertInto(nome,
                                                                                                    overwrite=False)


class Gerenciador:
    _dados_io = None
    _spark_session = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()

    def gera_regras(self):
        resultado = insert_tabela(self._spark_session)
        return resultado

    # def spark_session(self):
    #     return DadosIO._spark


def insert_tabela(spark):
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    try:
        cd_sequencial = sqlContext.sql(
            'select max(cd_sequencial) as max from analytical.a_estrutural_log_validacao_carga_prevista where nm_processo="{}" and nm_objeto="{}" and dt_execucao={}'.format(
                nm_processo, nm_objeto, dt_verificacao))
        cd_sequencial = cd_sequencial.collect()[0]['max']
        print(cd_sequencial)
    except:
        cd_sequencial = None
    if cd_sequencial == None:
        cd_sequencial = 1
    else:
        cd_sequencial = int(cd_sequencial)
    df = sqlContext.sql(
        'select count(*) as qt_registros_inseridos from harmonized.{} where dt_carga in (select max(dt_carga) from harmonized.{})'.format(
            nm_objeto, nm_objeto))
    df = (df
          .withColumn('cd_sequencial', sf.lit(cd_sequencial))
          .withColumn('nm_processo', sf.lit(nm_processo))
          .withColumn('nm_objeto', sf.lit(nm_objeto))
          .withColumn('dt_execucao', sf.lit(dt_verificacao))
          )
    df.show()

    df = (
        df.select(
            df['cd_sequencial'].cast(IntegerType()).alias('cd_sequencial'),
            df['nm_processo'],
            df['nm_objeto'],
            sf.trim(df['qt_registros_inseridos']).cast('bigint').alias('qt_registros_inseridos'),
            df['dt_execucao'].cast('bigint').alias('dt_execucao'),
        )
            .withColumn('aa_carga', sf.lit(dt_verificacao[:4]))
    )
    df.show()
    return df


_dados_io = DadosIO('RED_FINANCEIRA')
_gerenciador = Gerenciador(_dados_io)
df = _gerenciador.gera_regras()

tabela_hive = 'analytical.a_estrutural_log_validacao_carga_inserida'
_dados_io.gravar_parquet(df, tabela_hive)
######## -*- coding: utf-8 -*-#########
############ Versão - 1.0 ###########
####### Log - Registros Realizados #####
######## Alex Fernando dos S Silva ####
################################

import sys
import os
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

nm_processo = str(sys.argv[1])
nm_objeto = str(sys.argv[2])
dt_verificacao = str(sys.argv[3])

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("log_registros_realizados") \
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
        _dados_io = None
        _spark_session = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._spark_session = _dados_io.spark_session()

        def gera_regras(self):
            resultado = insert_tabela('teste')
            return resultado

    # Função que realiza os tratamentos de datatypes e colocar a string em maiúsculo.
    def insert_tabela(teste):
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
        df = sqlContext.sql('select count(*) as qt_registros_inseridos from harmonized.{} where dt_carga={}'.format(nm_objeto, dt_verificacao))
        df = (df
            .withColumn('cd_sequencial', sf.lit(cd_sequencial))
            .withColumn('nm_processo', sf.lit(nm_processo))
            .withColumn('nm_objeto', sf.lit(nm_objeto))
            .withColumn('dt_execucao', sf.lit(dt_verificacao))
        )

        df = (
            df.select(
                df['cd_sequencial'].cast(IntegerType()).alias('cd_sequencial'),
                df['nm_processo'],
                df['nm_objeto'],
                sf.trim(df['qt_registros_inseridos']).cast('bigint').alias('qt_registros_inseridos'),
                df['dt_execucao'].cast('bigint').alias('dt_execucao'),
            )
            .withColumn('aa_carga',sf.lit(dt_verificacao[:4]))
        )

        return df

    _dados_io = ArquivosDadosIO('RED_FINANCEIRA')
    _dados_io2 = TabelasDadosIO('RED_FINANCEIRA')

    _gerenciador = Gerenciador(_dados_io)
    df = _gerenciador.gera_regras()

    tabela_hive = 'analytical.a_estrutural_log_validacao_carga_inserida'
    _dados_io2.gravar_parquet(df, tabela_hive)

    spark.stop()
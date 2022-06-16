# -*- coding: utf-8 -*-
import pyspark
import sys
import os
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
from datetime import datetime, timedelta
from pyspark.sql.types import *
import logging
import time
import pandas as pd

logcopy = sys.argv[1]
processo = sys.argv[2]

def gera_log_execucao(processo, dt_inicio, dt_termino, tempoexecucao, log_msg):
    _spark = SparkSession.builder.appName('log_execucao') \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    aa_carga = str(datetime.today().strftime('%Y'))
    log_msg = [(processo, dt_inicio, dt_termino, tempoexecucao, log_msg, aa_carga)]

    tabela_log_insert = _spark.createDataFrame(log_msg, ["nm_processo",
                                                         "dt_inicio_execucao",
                                                         "dt_fim_excucao",
                                                         "tt_execucao",
                                                         "ds_status",
                                                         "aa_carga"])
    tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("datainsert") \
        .getOrCreate()

    pos_ini = logcopy.find('ErrorCode')
    pos_fin = logcopy[pos_ini::].find(',')
    pos_error = logcopy[pos_ini:  pos_ini + pos_fin]
    print(pos_error)

    dt_pos_str = logcopy.find('start":"')
    dt_pos_str_f = logcopy[dt_pos_str::].find('.')
    dt_str = logcopy[dt_pos_str + len('start":"'): dt_pos_str + dt_pos_str_f]
    print(dt_str)

    dur_ini = logcopy.find('duration":')
    dur_fin = logcopy[dur_ini::].find(',')
    durat = logcopy[dur_ini + len('duration":'): dur_ini + dur_fin]
    print(durat)

    ini = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
    fim = ini + timedelta(seconds=int(durat))
    vari = datetime.strptime(durat, '%S').strftime('%H:%M:%S')
    gera_log_execucao(processo, ini, fim, vari, pos_error)

    

    spark.stop()
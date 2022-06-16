#*******************************************************************************************
# NOME: BATCH_FASTDATA.PY
# OBJETIVO: CARREGAR NO DIRETORIO DETALHADO AS INFORMACOES QUE SERAO PROCESSADAS E FAZER UM
#           MERGE DOS ARQUIVOS EM UM UNICO ARQUIVO QUE SERA UTILIZADO PARA A CARGA NO HIVE.
# CONTROLE DE VERSAO
# 30/07/2019 - VERSAO INICIAL - ARIANE TATEISHI
#*******************************************************************************************
######## -*- coding: utf-8 -*-########

import subprocess
import sys
import logging
import json
import pyspark.sql
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

processo = str(sys.argv[1])
processo_pipe = str(sys.argv[2])

if __name__ == "__main__":
   conf = SparkConf().setAppName("BatchFastData")
   sc = SparkContext(conf=conf)
   spark = SparkSession.builder.appName("BatchFastData").getOrCreate()

def gera_log_execucao(processo_pipe, dt_inicio, dt_termino, tempoexecucao, log_msg):
     _spark = SparkSession.builder.appName('log_execucao') \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

     aa_carga = str(datetime.datetime.today().strftime('%Y'))
     log_msg = [(processo_pipe, dt_inicio, dt_termino, tempoexecucao, log_msg, aa_carga)]

     tabela_log_insert = _spark.createDataFrame(log_msg, ["nm_processo",
                                                           "dt_inicio_execucao",
                                                           "dt_fim_excucao",
                                                           "tt_execucao",
                                                           "ds_status",
                                                           "aa_carga"])
     tabela_log_insert.show()
     tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")

#Carrega as variaveis de configuracao
def carregaLib():
    _getLibs = "/data/source/lib"
    cmd = ['hdfs', 'dfs', '-get', _getLibs, '.']
    subprocess.check_output(cmd).strip().split('\n')
    sys.path.insert(0, "lib")
carregaLib()
import Config.Constants as conf

# Configura as variaveis que serao utilizadas para montar o nome dos diretorios.
ano = str(datetime.datetime.now().year)
mes = str(datetime.datetime.now().month).zfill(2)
dia = str(datetime.datetime.now().day).zfill(2)

# recupera as informacos do arquivo de configuracao, considerando o parametro de entrada

v_blob_spark = conf.getBlobUrl('bloblake')#azredlake

name_folder = conf.getFolderName(processo)
name_path = conf.getPathName(processo)
name_file = conf.getFileName(processo)

df3 = None
dt_inicio = str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.datetime.strptime(dt_inicio, fmt)
try:
    # Lista os arquivos existente no diretorio no qual o streaming esta sendo gerado.
    path_1 = v_blob_spark+"/data/raw/{}/{}/streaming/*.csv".format(name_folder, name_path)
    args = "hdfs dfs -ls "+path_1+" | awk '{print $9}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()

    # Cria o diretorio no qual a informacao detalhada sera armazenada.
    path_2 = v_blob_spark+"/data/raw/{}/{}/detalhado/".format(name_folder, name_path)
    args_mkdir = "hdfs dfs -mkdir "+path_2+ano+mes+dia
    print(args_mkdir)
    proc_mkdir = subprocess.Popen(args_mkdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output_mkdir, s_err_mkdir = proc_mkdir.communicate()

    # Para cada arquivo existe na pasta que esta sendo gerado o streaming, move o arquivo para a pasta de detalhada.
    for line in all_dart_dirs:
        path_3 = v_blob_spark+"/data/raw/{}/{}/detalhado/".format(name_folder, name_path)
        args_move = "hdfs dfs -mv "+line+" "+path_3+ano+mes+dia
        proc_move = subprocess.Popen(args_move, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output_move, s_err_move = proc_move.communicate()
        print(line)

    # Recupera os arquivos da pasta historico e faz um merge, depois copia este arquivo para a pasta consolidado.
    path_4 = "/data/raw/{}/{}/detalhado/".format(name_folder, name_path)
    allfiles =  spark.read.option("header","false").csv(path_4+ano+mes+dia+"/part-*.csv")
    path_5 = "/data/raw/{}/{}/consolidado/".format(name_folder, name_path)
    allfiles.coalesce(1).write.format("csv").option("header", "false").mode("overwrite").save(path_5+ano+mes+dia)

    # Renomeia o arquivo gerado para o padrao a ser recebido no data factory.
    path_6 = v_blob_spark+"/data/raw/{}/{}/consolidado/".format(name_folder, name_path)
    file_1 = "/{}.csv".format(name_file)
    args_rename = "hdfs dfs -mv "+path_6+ano+mes+dia+"/*.csv "+path_6+ano+mes+dia+file_1
    proc_rename = subprocess.Popen(args_rename, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output_rename, s_err_rename = proc_rename.communicate()
except Exception as ex:
   df3 = 'ERRO: {}'.format(str(ex))
   dt_fim = str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
   d2 = datetime.datetime.strptime(dt_fim, fmt)
   diff = d2 - d1
   tempoexec = str(diff)
   gera_log_execucao(processo_pipe, dt_inicio, dt_fim, tempoexec, df3)
   spark.stop()
   exit(1)
else:
   df3 = ("succeeded")
   dt_fim = str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
   d2 = datetime.datetime.strptime(dt_fim, fmt)
   diff = d2 - d1
   tempoexec = str(diff)
   gera_log_execucao(processo_pipe, dt_inicio, dt_fim, tempoexec, df3)
   spark.stop()
   exit(0)

######################################
######## -*- coding: utf-8 -*-########
#########ALL CHECK -FICO #############
####### MOVE ARQUIVO -HISTORICO #######
############# Main ####################
#############Versão - 1.0##############
#######################################

import subprocess
import sys
import logging
import json
import pyspark.sql
import datetime
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

name_folder = str(sys.argv[1])
name_path = str(sys.argv[2])
# name_file = str(sys.argv[3])

v_blob_spark = "abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net"

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("tratabatch")\
        .getOrCreate()

    # Configura as variaveis que serao utilizadas para montar o nome dos diretorios.
    ano = str(datetime.datetime.now().year)
    mes = str(datetime.datetime.now().month).zfill(2)
    dia = str(datetime.datetime.now().day).zfill(2)

    print('le arquivos do diretorio streaming')
    # Lista os arquivos existente no diretorio no qual o streaming esta sendo gerado.
    path_1 = v_blob_spark+"/data/raw/{}/{}/streaming/*.txt".format(name_folder, name_path)
    args = "hdfs dfs -ls "+path_1+" | awk '{print $9}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.split()

    print('cria diretorio historico')
    # Cria o diretorio no qual a informacao historica sera armazenada.
    # args_mkdir = "hdfs dfs -mkdir abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadPreAnaliseOutput/historico/"+ano+mes+dia
    path_2 = v_blob_spark+"/data/raw/{}/{}/historico/".format(name_folder, name_path)
    args_mkdir = "hdfs dfs -mkdir "+path_2+ano+mes+dia
    print(args_mkdir)
    proc_mkdir = subprocess.Popen(args_mkdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output_mkdir, s_err_mkdir = proc_mkdir.communicate()

    print('move arquivo para o diretorio historico')
    # Para cada arquivo existe na pasta que esta sendo gerado o streaming, move o arquivo para a pasta de historico.
    for line in all_dart_dirs:
        path_3 = v_blob_spark+"/data/raw/{}/{}/historico/".format(name_folder, name_path)
        args_move = "hdfs dfs -mv "+line+" "+path_3+ano+mes+dia
        proc_move = subprocess.Popen(args_move, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output_move, s_err_move = proc_move.communicate()
        # print(line)

print('fim do processo')
spark.stop()

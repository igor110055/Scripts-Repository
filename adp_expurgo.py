######################################
######## -*- coding: utf-8 -*-########
#############Versão - 1.0#############
######### ROTINA DE EXPURGO ##########
############ Victor Buoro ############
######################################


from pyspark.sql import HiveContext

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
from sys import argv
import os
#import foo
import subprocess
#from azure.storage.file import FileService
# from DBUtils import dbutils
# import PooledDB
# import pgdb
#--------------
import os
import time
#--------
from datetime import datetime
import logging
import shutil
import gzip

jobs = {}

####################################################
# Leitura de Arquivos Locais ou Banco de Dados     #
# autor: Daniel Lopes Barreiros                    #
####################################################
def sleep(sec):
    from time import sleep
    print "sleep"
    sleep(sec)
    print "wake"

class DadosIOLakeStorage:
    _spark = None
    _fs = None

    def __init__(self, job):
        if DadosIOLakeStorage._spark is None:
            _spark = SparkSession.builder.getOrCreate()

    def spark_session(self):
        return DadosIOLakeStorage._spark

    def readPathLakeStorageCSV(self, containers, nm_folder):
        url_folder = DadosIOLakeStorage.getPathLakeStorage(containers, nm_folder)
        return DadosIOLakeStorage.read.csv(url_folder, header=True, sep=";")

    def getPathLakeStorage(self, containers, nm_folder):
        return "abfs://{}@azredlake.dfs.core.windows.net/{}/".format(containers, nm_folder)


class DadosIOBlobStorage:
    _spark = None
    _fs = None

    def __init__(self, job):
        if DadosIOBlobStorage._spark is None:
            _spark = SparkSession.builder \
                .config("fs.azure.account.key.azreddevstgacc.blob.core.windows.net",
                        "uxEa+N5G+94z+0/UNdOfdJbBkn7iQ4pPOZ0FNgU14WNKsVIy7sHA/T1arnyxGMFB46KBFTsHtNr2jQr2pl64Hg==") \
                .getOrCreate()

    def spark_session(self):
        return DadosIOBlobStorage._spark

    def readPathBlobStorageCSV(self, containers, nm_folder):
        path = DadosIOBlobStorage.getPathBlobStorage(containers, nm_folder)
        return DadosIOBlobStorage.read.csv(path, header=True, sep=";")

    def getPathBlobStorage(self, containers, nm_folder):
        return "wasbs://{}t@azreddevstgacc.blob.core.windows.net/{}".format(containers, nm_folder)


class DadosIO:
    # {
    _spark = None
    _fs = None

    def __init__(self, job):
        if DadosIO._spark is None:
            DadosIO._spark = SparkSession.builder.appName(job) \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .config("spark.sql.parquet.writeLegacyFormat", "true") \
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                .enableHiveSupport() \
                .getOrCreate()

    def spark_session(self):
        return DadosIO._spark

    def parametros(self, processo, camada):
        _param = DadosIO._spark.table('harmonized.h_estrutural_expurgo') \
            .filter("nm_processo = '" + processo + "'") \
            .filter("nm_camada = '" + camada + "'") \
            .select('*')
        _param.createOrReplaceTempView("parametros")
        return _param.collect()

    def parametrosAll(self):
        _campos = ['nm_processo',
                   'nm_camada',
                   'nm_database',
                   'nm_tabela',
                   'nm_dir_origem',
                   'nm_dir_historico',
                   'nm_particao',
                   'fl_ativo']
        _param = DadosIO._spark.table('harmonized.h_estrutural_expurgo') \
            .filter("fl_ativo = 'S'") \
            .select(_campos)
        _param.createOrReplaceTempView("parametros")
        return _param.collect()

    def save_table_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header", "false").insertInto(
            nome, overwrite=True)

    def save_archive_parquet_old(self, df, path, partition):
        df.read.csv(path).coalesce(partition).write.partitionBy(partition).parquet('output').output()

    def save_archive_parquet_in_zg(self, df, name_table):
        df.write.mode("append") \
            .option("encoding", "UTF-8") \
            .option("header", "false") \
            .format("parquet") \
            .saveAsTextFile(name_table, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    def gravar_csv(self, df, path, nome):
        df.repartition(1).write.csv(path, header=True, mode='overwrite', sep='|', quote='"')
        arq = self._fs.globStatus(self._spark._jvm.Path(path + '/part*'))[0].getPath().getName()

    def readParque(self, _path_and_name_archive):
        return DadosIO._spark.read.parquet(_path_and_name_archive)


# }
####################################################
# FIM eitura de Arquivos Locais ou Banco de Dados  #
####################################################
# ----------------------------------------------------------
#####################################################
# Functions utis                                    #
#####################################################
logger = logging.getLogger("")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: FIN_EXPURGO %(message)s'
)


def setError(msg):
    # print(msg)
    logger.info(msg)
    sys.exit(1)


def setInfo(msg):
    # print(msg)
    logger.info(msg)


def debug(printDebug, stop=True):
    print("Debug:")
    print printDebug

    if (stop):
        print("Debug-Stop:Execução!")
        os._exit(0)


def isArchiveExist(path_and_name_archive):
    return os.path.exists(path_and_name_archive)


def listFilesHDFS(path):
    '''
    :param path: diretorio que deseja listar os arquivos
    :return: lista com os arquivos no diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-ls', '-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files


def moveFilesHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-mv', src, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def copyFilesHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-cp', src, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def getFileHDFS(src):
    '''
    :param src: arquivo com diretorio do HDFS
    :return: coloca o arquivo na pasta de trabalho do servidor (local onde está sendo executado o job)
    '''
    cmd = ['hdfs', 'dfs', '-get', src, '.']
    return subprocess.check_output(cmd).strip().split('\n')


def putFileHDFS(nome_arquivo, dst):
    '''
    :param nome_arquivo: nome do arquivo a ser colocado no HDFS
    :param dst: Diretorio do HDFS que deseja colocar o arquivo
    :return: insere o arquivo no HDFS
    '''
    cmd = ['hdfs', 'dfs', '-put', nome_arquivo, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def removeFileHDFS(src):
    '''
    :param src: nome do arquivo com diretorio no HDFS
    :return: remove arquivo do HDFS
    '''
    if (src != '/'):
        cmd = ['hdfs', 'dfs', '-rm', '-r', src]
        return subprocess.check_output(cmd).strip().split('\n')
    return False


def removeFile(src):
    '''
    :param src: nome do arquivo com diretorio
    :return: remove arquivo do servidor
    '''
    return os.remove(src)


#####################################################
# FIM Functions utis                                #
#####################################################

def expurgo_raw(processo,_camada):
    '''
    :return: Move os arquivos na raw para o diretorio de historico no formato .gz
    '''
    try:
        _dados_io = DadosIO('Expurgo')
        _param = _dados_io.parametros(processo, _camada)
        _diretorio_local = './'
        for _parametro in _param:
            _diretorios_origem = _parametro['nm_dir_origem']
            _diretorios_historico = _parametro['nm_dir_historico']
            _database_name = _parametro['nm_database']
            _table_name = _parametro['nm_tabela']
            _partition_name = _parametro['nm_particao']

            path_raw_diario = _diretorios_origem
            path_raw_historico = _diretorios_historico

            diretorios = listFilesHDFS(path_raw_diario)

            for diretorio in diretorios:
                diretorio_relativo = diretorio[len(path_raw_diario):]
                if len(diretorio_relativo) > 1:
                    mes_diretorio = datetime.strptime(diretorio_relativo, '%Y%m%d').strftime("%Y%m")
                    if mes_diretorio <= odate:
                        # moveFilesHDFS(diretorio, path_raw_historico)
                        moveFilesHDFS(diretorio, path_raw_historico)
                        arquivos = listFilesHDFS(os.path.join(path_raw_historico, diretorio_relativo))
                        for arquivo in arquivos:
                            # Nome exato do arquivo ( +1 eh para remover a barra antes do nome do arquivo )
                            nome_arquivo = arquivo[len(os.path.join(path_raw_historico, diretorio_relativo)) + 1:]
                            # Recupera arquivo do HDFS
                            getFileHDFS(arquivo)
                            # comprime arquivo
                            with open(os.path.join('./', nome_arquivo), 'rb') as f_in, gzip.open(
                                    '{}.gz'.format(nome_arquivo), 'w') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                            # transfere arquivo comprimido para HDFS
                            putFileHDFS('./{}.gz'.format(nome_arquivo),
                                        os.path.join(path_raw_historico, diretorio_relativo))
                            # Remover o arquivo da raiz e o txt do hdfs, mantendo apenas a versao comprimida
                            removeFileHDFS(arquivo)
                            removeFile(os.path.join('./', nome_arquivo))
    except Exception as ex:
        setError('Falha ao realizar o expurgo. {}'.format(ex))

# FIM expurgo_raw
def expurgo_harmonized(processo,_camada):
    '''
    :return: Move os arquivos na raw para o diretorio de historico no formato .gz
    '''
    try:
        _dados_io = DadosIO('Expurgo')
        _param = _dados_io.parametros(processo,_camada)
        _diretorio_local = './'
        for _parametro in _param:
            _diretorios_origem = _parametro['nm_dir_origem']
            _diretorios_historico = _parametro['nm_dir_historico']
            _database_name = _parametro['nm_database']
            _table_name = _parametro['nm_tabela']
            _partition_name = _parametro['nm_particao']

            diretorios = listFilesHDFS(_diretorios_origem)
            for diretorio in diretorios:
                diretorio_relativo = diretorio[len(_diretorios_origem):]
                if len(diretorio_relativo) > 1:
                    mes_diretorio = datetime.strptime(diretorio_relativo, '%Y%m%d').strftime("%Y%m")
                    if mes_diretorio <= odate:
                        moveFilesHDFS(diretorio, _diretorios_historico)
                        subdiretorio_hist = os.path.join(_diretorios_historico, diretorio_relativo)
                        subdiretorios = listFilesHDFS(subdiretorio_hist)

                        for subdiretorio in subdiretorios:
                            arquivos = listFilesHDFS(subdiretorio)
                            for arquivo in arquivos:
                                # Nome exato do arquivo ( +1 eh para remover a barra antes do nome do arquivo )
                                nome_arquivo = arquivo[len(subdiretorio) + 1:]
                                if nome_arquivo != "_SUCCESS":
                                    # Recupera arquivo do HDFS
                                    getFileHDFS(arquivo)
                                    # comprime arquivo
                                    with open(os.path.join(_diretorio_local, nome_arquivo), 'rb') as f_in, gzip.open(
                                        '{}.gz'.format(nome_arquivo), 'w') as f_out:
                                        shutil.copyfileobj(f_in, f_out)
                                    # transfere arquivo comprimido para HDFS
                                    putFileHDFS('./{}.gz'.format(nome_arquivo), subdiretorio)
                                    # Remover o arquivo da raiz e o txt do hdfs, mantendo apenas a versao comprimida
                                    removeFileHDFS(arquivo)
                                    removeFile(os.path.join(_diretorio_local, nome_arquivo))

    except Exception as ex:
        setError('Falha ao realizar o expurgo. {}'.format(ex))

# FIM expurgo_harmonized

def expurgo_tabela(processo,_camada):
    '''
        :return: Move os arquivos na raw para o diretorio de historico no formato .gz
        '''
    try:
        _dados_io = DadosIO('Expurgo')
        _param = _dados_io.parametros(processo, _camada)
        _diretorio_local = './'

        for _parametro in _param:
            _diretorios_origem = _parametro['nm_dir_origem']
            _diretorios_historico = _parametro['nm_dir_historico']
            _database_name = _parametro['nm_database'].lower()
            _table_name = processo.lower()
            _table_name_historico = _parametro['nm_tabela'].lower()
            _partition_name = _parametro['nm_particao'].lower()

            diretorios_da_origem = listFilesHDFS(_diretorios_origem)
            for arq in diretorios_da_origem:
                data_partition = arq.split("=")
                leng = len(data_partition)
                if (leng > 1):
                    _partition = data_partition[1]
                    _partition_atual = ""
                    mes_diretorio = datetime.strptime(_partition, '%Y%m%d').strftime("%Y%m")
                    if (int(mes_diretorio) <= int(odate)):
                        if(len(_partition_name)>0):
                            _partition_atual = _partition_name + '=' + _partition
                        moveFilesHDFS('{}{}'.format(_diretorios_origem, _partition_atual), _diretorios_historico)
                        # Remove particao da tabela
                        hive = HiveContext(_dados_io.spark_session())
                        drop_partition = "ALTER TABLE {}.{} DROP PARTITION ({})".format(_database_name, _table_name, _partition_atual)
                        hive.sql(drop_partition)
                        _diretorios_historico_with_partition = _diretorios_historico + _partition_atual

                        arquivos_particao = listFilesHDFS(_diretorios_historico_with_partition)

                        for path_name_arquivo in arquivos_particao:

                            if (path_name_arquivo.find('.parquet') > 0):
                                _diretorios_historico_with_partition_tmp = _diretorios_historico_with_partition + '/tmp'

                                df = _dados_io.readParque(path_name_arquivo)
                                df.write.csv(_diretorios_historico_with_partition_tmp, header=False, mode='overwrite',
                                             sep='|', compression="gzip")

                                diretorios_with_csv = listFilesHDFS(_diretorios_historico_with_partition_tmp)
                                removeFileHDFS(path_name_arquivo)

                                for arq_CSV in diretorios_with_csv:

                                    if (arq_CSV.find(".csv") > 0):
                                        moveFilesHDFS(arq_CSV, _diretorios_historico_with_partition)
                                        removeFileHDFS(_diretorios_historico_with_partition_tmp)
                                        # Atualiza particoes da historico
                                        atualiza_particoes = "MSCK REPAIR TABLE {}.{}".format(_database_name, _table_name_historico)
                                        # hive = HiveContext(_dados_io.spark_session())
                                        hive.sql(atualiza_particoes)

                                # fim for
                        # fim for
            # fim for
        # fim For
    except Exception as ex:
       setError('Falha ao realizar o expurgo. {}'.format(ex))

    return True
# FIM expurgo_tabela



# ----------------------------------------------------------
# CONSTANTS
# ----------------------------------------------------------
odate    = argv[1]
processo = argv[2]
camada = argv[3]
sis_unitario = argv[4]

# odate:201906, processo:parcelas, camada:raw, sis_unitario:1
if(len(sis_unitario) > 0 and sis_unitario == '1' and len(argv[1:] ) == 4 ):
    try:
        if( camada.upper()=='TABELA' or
            camada.upper()=='RAW' or
            camada.upper()=='HARMONIZED'):
            setInfo( "--> Inicio do processo; Nome Processo:{} odate:{} camada:{} em {}"\
                 .format(processo, odate, camada, datetime.now()))
            exec("expurgo_{}(processo,camada)".format(camada.lower()))
    except Exception as ex:
        print "erro: {}".format(ex)
        sleep(30)
else:
    if len(odate) == 0 :
        setInfo("--> Falta de parametro data ({}, formato yyyyMM ), não encontrada! ".format(odate) )
    _dados_io = DadosIO('Expurgo all')
    _paramAll = _dados_io.parametrosAll()
    for param in _paramAll:
        processo = param['nm_processo']
        camada = param['nm_camada']
        # setInfo("--> Inicio do processo; Nome Processo:{} odate:{} camada:{} em {}".format(processo, odate, camada, datetime.now()))
        # Expurgo de dados da RAW
        if (camada.upper() == "RAW"):
            expurgo_raw(processo,camada)
        # Expurgo de dados da HARMONIZED
        elif (camada.upper() == "HARMONIZED"):
            expurgo_harmonized(processo,camada)
        # Expurgo de dados da TABELA
        elif (camada.upper() == "TABELA"):
            expurgo_tabela(processo,camada)
        # Parametro de camada invalido
        # else:
        #     exit(1)
setInfo('--> FIM')

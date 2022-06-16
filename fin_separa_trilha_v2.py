######## -*- coding: utf-8 -*-########
# *******************************************************************************************
# NOME: FIN_SEPARA_TRILHA_V2.PY
# OBJETIVO: REALIZA O TRATAMENTO DE SEPARA TRILHAS PARA FINANCEIRA.
# CONTROLE DE VERSAO
# 20/05/2019 - ALEX SILVA - VERSAO INICIAL
# *******************************************************************************************

import sys
import os
import subprocess
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

caminho_origem = str(sys.argv[1])
caminho_destino = caminho_origem.replace('diario', 'trilhas')
detalhes = str(sys.argv[2])
nm_processo = str(sys.argv[3])
nm_objeto = str(sys.argv[4])
dt_verificacao = str(sys.argv[5])
_path_folder = caminho_origem.split('/')[-1]


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

    def get_data(self):
        return DadosIO._spark.read.text('{}/{}/'.format(caminho_origem, dt_verificacao))

    def gravar_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8").format("parquet").option("header", "false").insertInto(nome,
                                                                                                    overwrite=False)


class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _spark_session = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()
        self._fonte = _dados_io.get_data()

    def gera_regras(self):
        resultado = separa_detalhes(self._fonte)

        resultado2 = fonte_log(resultado)

        return resultado2


def separa_detalhes(df_fonte):
    url = ''
    url_entrada = '{}/{}/'.format(caminho_origem, dt_verificacao)
    files = listFilesHDFS(url_entrada)
    nomeAtual = getNameFiles(files)[0]
    "nome do arquivo para processar: {}".format(nomeAtual)

    file_original = df_fonte.withColumn('novacoluna', sf.substring('value', 0, 1))
    for f in detalhes:
        url = '{}/diario/{}/{}{}'.format(caminho_destino, dt_verificacao, _path_folder, f)
        output = file_original.filter(file_original.novacoluna.isin(f))
        output = output.select('value')
        output.write.mode('overwrite').text(url)

    files_saida = listFilesHDFS(url)
    nomeGerado = getNameFiles(files_saida)
    print "param {} , {} , {}".format(url, nomeGerado, nomeAtual)
    rename(url, nomeGerado, nomeAtual)

    return df_fonte


def fonte_log(fonte):
    sc = DadosIO._spark.sparkContext
    sqlContext = SQLContext(sc)
    try:
        cd_sequencial = sqlContext.sql(
            'select max(cd_sequencial) as max from analytical.a_estrutural_log_validacao_carga_prevista where nm_processo="{}" and nm_objeto="{}" and dt_execucao={}'.format(
                nm_processo, nm_objeto, dt_verificacao))
        cd_sequencial = cd_sequencial.collect()[0]['max']
    except:
        cd_sequencial = None
    if cd_sequencial == None:
        cd_sequencial = 1
    else:
        cd_sequencial = int(cd_sequencial)
        cd_sequencial = cd_sequencial + 1

    file_original = fonte.withColumn('novacoluna', sf.substring('value', 0, 1))
    output = file_original.filter(file_original.novacoluna.isin('9'))
    file_original = file_original.filter(file_original.novacoluna.isin(detalhes))
    file_original = file_original.withColumn('um', sf.lit(1))
    try:
        qt = file_original.groupBy('um').sum().collect()[0][1]
    except:
        qt = 0
    df = (output
          .withColumn('cd_sequencial', sf.lit(cd_sequencial))
          .withColumn('nm_processo', sf.lit(nm_processo))
          .withColumn('nm_objeto', sf.lit(nm_objeto))
          .withColumn('qt_registros_previstos', sf.lit(qt))  # sf.substring(output['value'], 2, 8)
          .withColumn('dt_execucao', sf.lit(dt_verificacao))
          )
    df = (
        df.select(
            df['cd_sequencial'].cast(IntegerType()).alias('cd_sequencial'),
            df['nm_processo'],
            df['nm_objeto'],
            sf.trim(df['qt_registros_previstos']).cast('bigint').alias('qt_registros_previstos'),
            df['dt_execucao'].cast('bigint').alias('dt_execucao'),
        )
            .withColumn('aa_carga', sf.lit(dt_verificacao[:4]))
    )
    return df


def listFilesHDFS(path):
    '''
    :param path: diretorio que deseja listar os arquivos
    :return: lista com os arquivos no diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-ls', '-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files


def path_arquivo_leitura(fpath):
    files_urls = []
    _map_fpath = os.path.abspath(fpath)
    i = 0
    for folder, subfolders, files in os.walk(_map_fpath):
        path = os.path.abspath(folder)
        for file in files:
            filePath = path + "\\" + file
            if os.path.getsize(filePath) >= 0:
                files_urls.insert(i, filePath)
                i = i + 1
    return files_urls


def getNameFiles(path_name_file):
    lista = []
    i = 0
    for name_file in path_name_file:
        namelist = name_file.split("/")
        namelen = len(namelist) - 1
        lista.insert(i, namelist[namelen])
        i = i + 1
    return lista


def rename(path_destino, nomeGeracao, novo_nome):
    for nome in nomeGeracao:
        findname = nome.find("SUCCESS")

        if (findname == -1):
            # tmp
            pasta_destino = "{}/tmp".format(path_destino)
            new_name_tmp = "{}/{}".format(pasta_destino, novo_nome)
            createFolderHDFS("{}/tmp".format(path_destino))

            # destino
            new_name = "{}/{}".format(path_destino, novo_nome)
            name_file = "{}/{}".format(path_destino, nome)

            # move tmp
            moveFilesHDFS(name_file, new_name_tmp)
            moveFilesHDFS(new_name_tmp, new_name)

            # remove tmp
            removeFileHDFS(pasta_destino)


def createFolderHDFS(dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-mkdir', dst]
    return subprocess.check_output(cmd).strip().split('\n')


def moveFilesHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-mv', src, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def removeFile(src):
    '''
    :param src: nome do arquivo com diretorio
    :return: remove arquivo do servidor
    '''
    return os.remove(src)


def renameFileHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-mv', src, dst]
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


_dados_io = DadosIO('RED_FINANCEIRA_SEPARA_TRILHAS')
_gerenciador = Gerenciador(_dados_io)
df = _gerenciador.gera_regras()

tabela_hive = 'analytical.a_estrutural_log_validacao_carga_prevista'
_dados_io.gravar_parquet(df, tabela_hive)

deletar_sucesso = '{}/diario/{}/{}{}/_SUCCESS'.format(caminho_destino, dt_verificacao, _path_folder, detalhes)
if listFilesHDFS(deletar_sucesso):
    removeFileHDFS(deletar_sucesso)

DadosIO._spark.stop()
######################################
######## -*- coding: utf-8 -*-########
########### ARQUIVO PROPOSTAS ########
########### TRILHA 02-  ##############
########### ADPB05N - Main ###########
#############Versão - 1.0#############
######################################

# -*- coding: utf-8 -*-
import sys
import os
import subprocess
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from itertools import chain
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime

# Variável global que retorna a data atual.
current_date = dt.datetime.now().strftime('%d/%m/%Y')
print(current_date)

processo = sys.argv[1]


def gera_log_execucao(processo, dt_inicio, dt_termino, tempoexecucao, log_msg):
    _spark = SparkSession.builder.appName('log_execucao') \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    # aa_carga = str(2020)
    aa_carga = str(datetime.today().strftime('%Y'))
    log_msg = [(processo, dt_inicio, dt_termino, tempoexecucao, log_msg, aa_carga)]

    tabela_log_insert = _spark.createDataFrame(log_msg, ["nm_processo",
                                                         "dt_inicio_execucao",
                                                         "dt_fim_excucao",
                                                         "tt_execucao",
                                                         "ds_status",
                                                         "aa_carga"])
    tabela_log_insert.show()
    tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")


####################################################
# Leitura de Arquivos Locais                      #
# autor: Daniel Lopes Barreiros                   #
####################################################
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

    def df_dados(self, URL):
        try:
            return DadosIO._spark.read.csv(URL, header=True, mode='overwrite', sep=';')
        except:
            print("Erro:path do arquivo não encontrado! ")

    def gravar(self, df, nome):
        df.repartition(1).write.csv('dados/ultima_gravacao/' + nome + '/', header=True, mode='overwrite', sep=';')

    def path_arquivos(self):
        _pathFiles = "";
        URL_cetip = '/data/raw/BaldeManual/BoaVista/diario/' + str(datetime.today().strftime('%Y%m%d')) + '/'
        _pathFiles = self.listFilesHDFS(URL_cetip)
        return _pathFiles

    def gravar_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header", "false").insertInto(
            nome, overwrite=True)

    def listFilesHDFS(Self, path):
        try:
            '''
            :param path: diretorio que deseja listar os arquivos
            :return: lista com os arquivos no diretorio informado
            '''
            cmd = ['hdfs', 'dfs', '-ls', '-C', path]
            files = subprocess.check_output(cmd).strip().split('\n')
            return files[0]
        except:
            print("Erro: path in HDSF not found!")

    def testLlistFilesLocal(self, fpath):

        files_urls = None
        _map_fpath = os.path.abspath(fpath)
        # print(" ".join(teste))
        for folder, subfolders, files in os.walk(_map_fpath):
            path = os.path.abspath(folder)
            for file in files:
                filePath = path + "\\" + file
                if os.path.getsize(filePath) >= 0:
                    files_urls = filePath
                    # print filePath, str(os.path.getsize(filePath)) + "kB"
        # print files_urls
        return files_urls

    def getDataNameFile(self, urlStringFile, nameBaseFile):
        # debug(nameBaseFile)
        _data_file = urlStringFile.split(nameBaseFile)
        _data_arq = _data_file[1].split(".")
        _data_arquivo = str(_data_arq[0])
        return _data_arquivo

    def gravar_csv(self, df, nome):
        df_param = self.ler_parametros()
        pasta = df_param.where(df_param.chave == 'ReportAnalitico_Pasta').select('regra').collect()
        df.repartition(1).write.csv(pasta[0].regra + '/' + nome + '-temp', header=True, mode='overwrite', sep='|',
                                    quote='"')
        arq = self._fs.globStatus(self._spark._jvm.Path(pasta[0].regra + '/' + nome + '-temp/part*'))[0].getPath() \
            .getName()
        self._fs.delete(self._spark._jvm.Path(pasta[0].regra + '/' + nome))
        self._fs.rename(self._spark._jvm.Path(pasta[0].regra + '/' + nome + '-temp/' + arq),
                        self._spark._jvm.Path(pasta[0].regra + '/' + nome))
        self._fs.delete(self._spark._jvm.Path(pasta[0].regra + '/' + nome + '-temp'), True)


####################################################
# FIM Leitura de arquivos Locais                   #
####################################################
class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _df_dados = None
    _spark_session = None
    _path_arquivos = None

    # Dataframes originais
    _parametros = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()

    # chamada de todos os passos da ingestao
    def gera_regras(self):
        _urlFile = self._dados_io.path_arquivos()
        # debug(_urlFile)
        _df_dados = self._dados_io.df_dados(_urlFile)
        _nameDateFile = self._dados_io.getDataNameFile(_urlFile, "boavista_")
        _final = formata_colunas(_df_dados, _nameDateFile)
        return _final


# def debug(printDebug):
#     print("Debug:")
#     print(printDebug)
#     os._exit(0)


def formata_colunas(df, data_arquivo):
    return (
        df.select(
            f.lpad(df['cpf compr'], 11, '0').alias('no_cpf'),
            f.upper(df['scrcrdmer6v3alin']).alias('cd_score')
        ).withColumn('dt_referencia_base', f.lit(str(data_arquivo)))
    )

        #################################################
        #          Fim da Classe Gerenciador            #
        #################################################

        #################################################
        #          Inicio da execução do código         #
        #################################################

print('Inicio')
df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)
_dados_io = DadosIO('RED_BOA_VISTA')
_gerenciador = Gerenciador(_dados_io)
_final = _gerenciador.gera_regras()


try:
    print('gravacao')
    _tabela = "harmonized.h_baldemanual_boavista"

    _dados_io.gravar_parquet(_final, _tabela)
except Exception as ex:
    df3 = 'ERRO: {}'.format(str(ex))
else:
    df3 = ("succeeded")
finally:
    print(df3)
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)

    gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)


    # print("fim processo")
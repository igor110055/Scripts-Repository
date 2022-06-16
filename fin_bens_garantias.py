#######################################
######## -*- coding: utf-8 -*-#########
############ Versão - 1.0 #############
#### Financeira - Bens e Garantias ####
######## Vinicius Scaglione ###########
#######################################

import sys
import os
from itertools import chain
import subprocess
import traceback
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime


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

###########################################inicio do codigo
processo = sys.argv[1]
dt_carga = sys.argv[2]
print processo

# Variável global que retorna a data atual.
current_date = dt_carga
#dt.datetime.now().strftime('%d/%m/%Y')
print(current_date)


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

    def bens_e_garantias(self):
        return DadosIO._spark.read.text(
            '/data/harmonized/Financeira/Bens e Garantias/diario/FINB537/{}/FINB5371/'.format(
                str(current_date)))

    def objeto_financiado(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ';') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Lookup/Objeto_financiado/')

    def gravar_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8") \
            .format("parquet") \
            .option("header", "false") \
            .insertInto(nome, overwrite=True)

    def path_arquivos(self):
        URL = '/data/harmonized/Financeira/Bens e Garantias/diario/FINB537/{}/FINB5371/'.format(
            str(datetime.today().strftime('%Y%m%d')))
        _pathFiles = self.listFilesHDFS(URL)
        return _pathFiles

    def carregarArquivo(self, URL):
        try:
            return DadosIO._spark.read.text(URL)
        except:
            print("Erro:path do arquivo não encontrado! ")

    def retirarDataNameFile(self, urlStringFile, nameBaseFile):
        # debug(nameBaseFile)
        _data_file = urlStringFile.split(nameBaseFile)
        _data_arq = _data_file[1].split(".")
        _data_arquivo = str(_data_arq[0])
        return _data_arquivo

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


class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _spark_session = None

    # Dataframes originais
    _parametros = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()
        # self._bens_e_garantias = _dados_io.bens_e_garantias()

    def gera_regras(self):
        # ----------------------------------------------
        _urlFile = self._dados_io.path_arquivos()
        print "_urlFile {}".format(_urlFile)
        _nameDateFile = self._dados_io.retirarDataNameFile(_urlFile, "basebensgarantias_")
        print "_nameDateFile {}".format(_nameDateFile)
        _dados = _dados_io.carregarArquivo(_urlFile)
        # ----------------------------------------------
        resultado = bens_e_garantias(_dados, _nameDateFile)

        resultado2 = bens_e_garantias_transformacao(resultado)

        return resultado2


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
    # print log_msg
    # ut_f.debug("fim teste")
    tabela_log_insert = _spark.createDataFrame(log_msg, ["nm_processo",
                                                         "dt_inicio_execucao",
                                                         "dt_fim_excucao",
                                                         "tt_execucao",
                                                         "ds_status",
                                                         "aa_carga"])
    tabela_log_insert.show()
    tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")
    # tabela_log_insert.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header", "true").partitionBy('aa_carga').saveAsTable("analytical.a_estrutural_log_execucao_carga")


#  Função que retorna os dados do arquivo de Bens e Garantias e realiza o tratamento para quebra de colunas.
def bens_e_garantias(df_bens_e_garantias, _nameDateFile):
    return (df_bens_e_garantias
            .withColumn('TP_REGISTRO', f.substring(df_bens_e_garantias['value'], 1, 1))
            .withColumn('TP_MOVIMENTO', f.substring(df_bens_e_garantias['value'], 2, 1))
            .withColumn('DS_VEICULO_LEGAL', f.substring(df_bens_e_garantias['value'], 3, 3))
            .withColumn('CD_FILIAL', f.substring(df_bens_e_garantias['value'], 6, 5))
            .withColumn('NO_CONTRATO', f.substring(df_bens_e_garantias['value'], 11, 15))
            .withColumn('CD_SEQUENCIA_VEICULO', f.substring(df_bens_e_garantias['value'], 26, 2))
            .withColumn('DS_MARCA_VEICULO', f.substring(df_bens_e_garantias['value'], 28, 15))
            .withColumn('DS_MODELO_VEICULO', f.substring(df_bens_e_garantias['value'], 43, 20))
            .withColumn('AA_FABRICACAO', f.substring(df_bens_e_garantias['value'], 63, 4))
            .withColumn('AA_VEICULO', f.substring(df_bens_e_garantias['value'], 67, 4))
            .withColumn('DS_COMBUSTIVEL_VEICULO', f.substring(df_bens_e_garantias['value'], 71, 1))
            .withColumn('DS_COR_VEICULO', f.substring(df_bens_e_garantias['value'], 72, 20))
            .withColumn('NO_PLACA_VEICULO', f.substring(df_bens_e_garantias['value'], 92, 10))
            .withColumn('NO_CHASSIS', f.substring(df_bens_e_garantias['value'], 102, 20))
            .withColumn('NO_RENAVAM', f.substring(df_bens_e_garantias['value'], 122, 10))
            .withColumn('DT_APREENSAO_VEICULO', f.from_unixtime(
        f.unix_timestamp(f.substring(df_bens_e_garantias['value'], 132, 8), 'ddMMyyy'), 'yyyMMdd').cast(
        IntegerType()))
            .withColumn('DT_DEVOLUCAO_APREENDIDO', f.from_unixtime(
        f.unix_timestamp(f.substring(df_bens_e_garantias['value'], 140, 8), 'ddMMyyy'), 'yyyMMdd').cast(
        IntegerType()))
            .withColumn('DT_VENDA_VEICULO', f.from_unixtime(
        f.unix_timestamp(f.substring(df_bens_e_garantias['value'], 148, 8), 'ddMMyyy'), 'yyyMMdd').cast(
        IntegerType()))
            .withColumn('DT_DEVOLUCAO_VEICULO', f.from_unixtime(
        f.unix_timestamp(f.substring(df_bens_e_garantias['value'], 156, 8), 'ddMMyyy'), 'yyyMMdd').cast(
        IntegerType()))
            .withColumn('TP_APREENSAO_VEICULO', f.substring(df_bens_e_garantias['value'], 164, 1))
            .withColumn('TP_DEVOLUCAO_VEICULO', f.substring(df_bens_e_garantias['value'], 165, 1))
            .withColumn('VL_VENDA_VEICULO', f.concat(f.substring(df_bens_e_garantias['value'], 166, 15), f.lit('.'),
                                                     f.substring(df_bens_e_garantias['value'], 181, 2)))
            .withColumn('VL_MERCADO', f.concat(f.substring(df_bens_e_garantias['value'], 183, 15), f.lit('.'),
                                               f.substring(df_bens_e_garantias['value'], 198, 2)))
            .withColumn('VL_GARANTIA', f.concat(f.substring(df_bens_e_garantias['value'], 200, 15), f.lit('.'),
                                                f.substring(df_bens_e_garantias['value'], 215, 2)))
            .withColumn('NO_RENAVAM2', f.substring(df_bens_e_garantias['value'], 217, 12))
            .withColumn('FILLER', f.substring(df_bens_e_garantias['value'], 229, 22))
            .withColumn('dt_carga', f.lit(_nameDateFile))
            # .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
            .drop('value')
            )


# Função que realiza os tratamentos de datatypes e colocar a string em maiúsculo.
def bens_e_garantias_transformacao(bens_e_garantias):
    lookup_TP_MOVIMENTO = {"I": "INCLUSAO", "A": "ALTERACAO", "E": "EXCLUSAO"}
    map_TP_MOVIMENTO = f.create_map([f.lit(x) for x in chain(*lookup_TP_MOVIMENTO.items())])

    lookup_DS_VEICULO_LEGAL = {"001": "BANCO", "002": "LEASING"}
    map_DS_VEICULO_LEGAL = f.create_map([f.lit(x) for x in chain(*lookup_DS_VEICULO_LEGAL.items())])

    lookup_DS_COMBUSTIVEL_VEICULO = {
        'A': 'Alcool',
        'G': 'Gasolina',
        'D': 'Diesel',
        'S': 'gasolina+gas',
        'C': 'alcool+gas',
        'L': 'gasolina+alcool',
        'V': 'gasolina+alcool+gas'
    }
    map_DS_COMBUSTIVEL_VEICULO = f.create_map([f.lit(x) for x in chain(*lookup_DS_COMBUSTIVEL_VEICULO.items())])

    lookup_TP_APREENSAO_VEICULO = {'A': 'Amigavel', 'J': 'Judicial'}
    map_TP_APREENSAO_VEICULO = f.create_map([f.lit(x) for x in chain(*lookup_TP_APREENSAO_VEICULO.items())])

    lookup_TP_DEVOLUCAO_VEICULO = {'A': 'Amigavel', 'J': 'Judicial'}
    map_TP_DEVOLUCAO_VEICULO = f.create_map([f.lit(x) for x in chain(*lookup_TP_DEVOLUCAO_VEICULO.items())])

    return (bens_e_garantias
        .select(
        bens_e_garantias['TP_REGISTRO'].cast(IntegerType()),
        ut_f.limpa_espaco_branco(map_TP_MOVIMENTO[bens_e_garantias['TP_MOVIMENTO']]).alias('TP_OPERACAO'),
        ut_f.limpa_espaco_branco(map_DS_VEICULO_LEGAL[bens_e_garantias['DS_VEICULO_LEGAL']]).alias('DS_VEICULO_LEGAL'),
        bens_e_garantias['CD_FILIAL'].cast(IntegerType()),
        bens_e_garantias['NO_CONTRATO'].cast('bigint'),
        bens_e_garantias['CD_SEQUENCIA_VEICULO'].cast(IntegerType()),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['DS_MARCA_VEICULO'])).alias('DS_MARCA_VEICULO'),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['DS_MODELO_VEICULO'])).alias('DS_MODELO_VEICULO'),
        bens_e_garantias['AA_FABRICACAO'].cast(IntegerType()),
        bens_e_garantias['AA_VEICULO'].cast(IntegerType()),
        ut_f.limpa_espaco_branco(map_DS_COMBUSTIVEL_VEICULO[bens_e_garantias['DS_COMBUSTIVEL_VEICULO']]).alias(
            'DS_COMBUSTIVEL_VEICULO'),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['DS_COR_VEICULO'])).alias('DS_COR_VEICULO'),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['NO_PLACA_VEICULO'])).alias('NO_PLACA_VEICULO'),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['NO_CHASSIS'])).alias('NO_CHASSIS'),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['NO_RENAVAM'])).alias('NO_RENAVAM'),
        bens_e_garantias['DT_APREENSAO_VEICULO'].alias('DT_APREENSAO_VEICULO'),
        bens_e_garantias['DT_DEVOLUCAO_APREENDIDO'].alias('DT_DEVOLUCAO_APREENDIDO'),
        bens_e_garantias['DT_VENDA_VEICULO'].alias('DT_VENDA_VEICULO'),
        bens_e_garantias['DT_DEVOLUCAO_VEICULO'].alias('DT_DEVOLUCAO_VEICULO'),
        ut_f.limpa_espaco_branco(map_TP_APREENSAO_VEICULO[bens_e_garantias['TP_APREENSAO_VEICULO']]).alias(
            'TP_APREENSAO_VEICULO'),
        ut_f.limpa_espaco_branco(map_TP_DEVOLUCAO_VEICULO[bens_e_garantias['TP_DEVOLUCAO_VEICULO']]).alias(
            'TP_DEVOLUCAO_VEICULO'),
        bens_e_garantias['VL_VENDA_VEICULO'].cast(DecimalType(17, 2)),
        bens_e_garantias['VL_MERCADO'].cast(DecimalType(17, 2)),
        bens_e_garantias['VL_GARANTIA'].cast(DecimalType(17, 2)),
        ut_f.limpa_espaco_branco(f.upper(bens_e_garantias['NO_RENAVAM2'])).alias('NO_RENAVAM2'),
        ut_f.limpa_espaco_branco(bens_e_garantias['FILLER']).alias('FILLER'),
        bens_e_garantias['dt_carga']
        )
    )


df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)
try:
    _dados_io = DadosIO('RED_BENS_E_GARANTIAS')

    _gerenciador = Gerenciador(_dados_io)
    df_bens_e_garantias = _gerenciador.gera_regras()
    tabela_hive = 'harmonized.h_financeira_bens_e_garantias'
    df_bens_e_garantias.show()
    _dados_io.gravar_parquet(df_bens_e_garantias, tabela_hive)
except Exception as ex:
    df3 = 'ERRO: {} {}'.format(str(ex), str(traceback.print_exc()))

    print df3
else:
    df3 = "succeeded"
finally:
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)
    gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    # print("fim processo")

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

    def df_dados_posicional(self, URL):
        try:
            return DadosIO._spark.read.text(URL)
        except:
            print("Erro:path do arquivo não encontrado! ")

    def gravar(self, df, nome):
        df.repartition(1).write.csv('dados/ultima_gravacao/' + nome + '/', header=True, mode='overwrite', sep=';')

    def path_arquivos(self):
        _pathFiles = "";
        URL_cetip = '/data/raw/BaldeManual/Cetip/diario/' + str(datetime.today().strftime('%Y%m%d')) + '/'
        _pathFiles = self.listFilesHDFS(URL_cetip)
        return _pathFiles

    def gravar_parquet(self, df, nome):
        df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header", "false").insertInto(
            nome, overwrite=True)

    def clear_table_hdsf(self, nm_table):
        DadosIO._spark.sql('''DROP  TABLE IF EXISTS  harmonized.h_baldemanual_cetip''')
        DadosIO._spark.sql('''
            CREATE  EXTERNAL TABLE harmonized.h_baldemanual_cetip(
                no_cpf_cnpj STRING,
                tp_documento INT,
                ds_documento  STRING,
                fl_veiculo_atual_historico INT,
                ds_veiculo_atual_historico STRING,
                fl_veiculo_financiado STRING,
                dt_primeiras_pagto_dpvat INT,
                dt_ultimo_pagto_dpvat INT,
                dt_inclusao INT,
                dt_baixa INT,
                cd_uf_emplacamento STRING,
                dt_contrato INT,
                qt_meses INT,
                tp_restricao INT,
                ds_restricao STRING,
                ds_marca_modelo STRING,
                aa_modelo INT,
                tp_veiculo STRING,
                tp_tabela STRING,
                ds_tabela STRING,
                cd_tabela INT,
                vl_atual  String,
                dt_aquisicao INT,
                dt_venda INT,
                fl_autofinanciamento STRING,
                dt_referencia_base string
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            WITH  SERDEPROPERTIES('SERIALIZATION.ENCODING' = 'UTF-8')
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION 'abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/hive/warehouse/harmonized.db/h_baldemanual_cetip/'
        ''')
        return True

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

    def retirarDataNameFile(self, urlStringFile, nameBaseFile):
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
        _df_dados = self._dados_io.df_dados_posicional(_urlFile)
        _nameDateFile = self._dados_io.retirarDataNameFile(_urlFile, "basecetip_")
        # debug(_urlFile)
        _dados_transformacao = formata_colunas(_df_dados, _nameDateFile)
        _final = dados_transformacao(_dados_transformacao)
        return _final


def debug(printDebug):
    print("Debug:")
    print
    printDebug
    os._exit(0)


def formata_colunas(df, data_arquivo):
    # Atenção: ultimo layout enviado 24/06/2019
    return (df
            .withColumn('no_cpf_cnpj', f.substring(df['value'], 1, 14).cast(IntegerType()))
            .withColumn('tp_documento', f.substring(df['value'], 15, 1))
            .withColumn('ds_documento', f.substring(df['value'], 15, 1))
            .withColumn('fl_veiculo_atual_historico', f.substring(df['value'], 16, 1))
            .withColumn('ds_veiculo_atual_historico', f.substring(df['value'], 16, 1))
            .withColumn('fl_veiculo_financiado', f.substring(df['value'], 17, 1))
            # .withColumn('ds_veiculo_financiado', f.substring(df['value'], 17, 1))
            .withColumn('dt_primeiras_pagto_dpvat', f.substring(df['value'], 18, 8))
            .withColumn('dt_ultimo_pagto_dpvat', f.substring(df['value'], 26, 8))
            .withColumn('dt_inclusao', f.substring(df['value'], 34, 8))
            .withColumn('dt_baixa', f.substring(df['value'], 42, 8))
            .withColumn('cd_uf_emplacamento', f.substring(df['value'], 50, 2))
            .withColumn('dt_contrato', f.substring(df['value'], 52, 8))
            .withColumn('qt_meses', f.substring(df['value'], 60, 3))
            .withColumn('tp_restricao', f.substring(df['value'], 63, 2))
            .withColumn('ds_restricao', f.substring(df['value'], 63, 2))
            .withColumn('ds_marca_modelo', f.substring(df['value'], 65, 55))
            .withColumn('aa_modelo', f.substring(df['value'], 120, 4))
            .withColumn('tp_veiculo', f.substring(df['value'], 124, 30))
            .withColumn('tp_tabela', f.substring(df['value'], 154, 1))
            .withColumn('ds_tabela', f.substring(df['value'], 154, 1))
            .withColumn('cd_tabela', f.substring(df['value'], 155, 7))
            .withColumn('vl_atual', f.substring(df['value'], 162, 11))
            .withColumn('dt_aquisicao', f.substring(df['value'], 173, 8))
            .withColumn('dt_venda', f.substring(df['value'], 181, 8))
            .withColumn('fl_autofinanciamento', f.substring(df['value'], 189, 1))
            # .withColumn('ds_autofinanciamento', f.substring(df['value'], 189, 1))
            .withColumn('dt_referencia_base', f.lit(str(data_arquivo)))
            ).drop('value')


def dados_transformacao(df):
    lookup_tp_documento = {"1": "CPF", "2": "CNPJ"}
    mapping_tp_documento = f.create_map([f.lit(x) for x in chain(*lookup_tp_documento.items())])

    lookup_veic_isHistorico = {"1": "Atual", "2": "Historico"}
    mapping_veic_isHistorico = f.create_map([f.lit(x) for x in chain(*lookup_veic_isHistorico.items())])

    # lookup_veic_isFinanciado = {"S": "SIM, é Financiado", "2": "Não, é Financiado"}
    # mapping_veic_isFinanciado = f.create_map([f.lit(x) for x in chain(*lookup_veic_isFinanciado.items())])

    lookup_tp_restricao = {"01": "Leasing", "02": "Reserva de domínio", "03": "Alienação Fiduciária", "09": "Penhor"}
    mapping_tp_restricao = f.create_map([f.lit(x) for x in chain(*lookup_tp_restricao.items())])

    lookup_tp_tabela = {"F": "FIPE", "M": "MolicarDados fornecidos pelo cliente"}
    mapping_tp_tabela = f.create_map([f.lit(x) for x in chain(*lookup_tp_tabela.items())])

    # lookup_isAutoFinanciamento = {"S": "Sim, é AutoFinanciamento", "N": "Não é AutoFinanciamento"}
    # mapping_isAutoFinanciamento = f.create_map([f.lit(x) for x in chain(*lookup_isAutoFinanciamento.items())])

    return (
        df.select(
            f.lpad(df['no_cpf_cnpj'], 11, '0').alias('no_cpf'),
            df['tp_documento'].cast(t.IntegerType()),
            f.upper(mapping_tp_documento[df['ds_documento']]).alias('ds_documento'),
            df['fl_veiculo_atual_historico'].cast(t.IntegerType()),
            f.upper(mapping_veic_isHistorico[df['ds_veiculo_atual_historico']]).alias('ds_veiculo_atual_historico'),
            f.upper(df['fl_veiculo_financiado']).alias('fl_veiculo_financiado'),
            # f.upper(mapping_veic_isFinanciado[df['ds_veiculo_financiado']]).alias('ds_veiculo_financiado'),
            df['dt_primeiras_pagto_dpvat'].cast(t.IntegerType()).alias('no_cpf_cnpj'),
            df['dt_ultimo_pagto_dpvat'].cast(t.IntegerType()).alias('tp_documento'),
            df['dt_inclusao'].cast(t.IntegerType()).alias('ds_documento'),
            df['dt_baixa'].cast(t.IntegerType()).alias('fl_veiculo_atual_historico'),
            f.upper(df['cd_uf_emplacamento']).alias('cd_uf_emplacamento'),
            df['dt_contrato'].cast(t.IntegerType()).alias('dt_primeiras_pagto_dpvat'),
            df['qt_meses'].cast(t.IntegerType()),
            df['tp_restricao'].cast(t.IntegerType()),
            f.upper(mapping_tp_restricao[df['ds_restricao']]).alias('ds_restricao'),
            f.upper(df['ds_marca_modelo']).alias('ds_marca_modelo'),
            df['aa_modelo'].cast(t.IntegerType()),
            f.upper(df['tp_veiculo']).alias('tp_veiculo'),
            f.upper(df['tp_tabela']).alias('tp_tabela'),
            f.upper(mapping_tp_tabela[df['ds_tabela']]).alias('ds_tabela'),
            df['cd_tabela'].cast(t.IntegerType()),
            df['vl_atual'].cast(t.IntegerType()),
            df['dt_aquisicao'].cast(t.IntegerType()),
            df['dt_venda'].cast(t.IntegerType()),
            f.upper(df['fl_autofinanciamento']).alias('fl_autofinanciamento'),
            # f.upper(mapping_isAutoFinanciamento[df['ds_autofinanciamento']]).alias('ds_autofinanciamento'),
            df['dt_referencia_base']
        )
    )


#################################################
#          Fim da Classe Gerenciador            #
#################################################


#################################################
#          Inicio da execução do código         #
#################################################

print
'Inicio'
df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)
_dados_io = DadosIO('RED_CETIP')
_gerenciador = Gerenciador(_dados_io)
_final = _gerenciador.gera_regras()

try:
    _tabela = "harmonized.h_baldemanual_cetip"
    # _dados_io.clear_table_hdsf(_tabela)
    _dados_io.gravar_parquet(_final, _tabela)
except Exception as ex:
    df3 = 'ERRO: {}'.format(str(ex))
else:
    df3 = ("succeeded")
finally:
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)

    gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)


print
'finalizou'

# *******************************************************************************************
# NOME: FICO_ALLCHECK_ANALISE_FULL_OUTPUT.PY
# OBJETIVO: CARREGA AS INFORMACOES NA TABELA HIVE.
# CONTROLE DE VERSAO
# 30/07/2019 - VERSAO INICIAL - ANA SEGATELLI
# 07/08/2019 - Criacao funcao valida_diretorio-  Ana Segatelli
# *******************************************************************************************
######## -*- coding: utf-8 -*-########

import sys
import os
from imp import reload

from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime
import sys
import shlex, subprocess

processo = sys.argv[1]
assunto = sys.argv[2]



if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("trataharmonized") \
        .getOrCreate()

    # Variavel global que retorna a data atual.
    current_date = dt.datetime.now().strftime('%d/%m/%Y')
    print(current_date)


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
        tabela_log_insert.show()
        tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")


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

        print(assunto)

        if assunto == ('dados_cadastrais'):
            def dados_cadastrais(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('obitos'):
            def obitos(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('sociedades'):
            def sociedades(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('telefone'):
            def telefone(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('empresas'):
            def empresas(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('veiculos'):
            def veiculos(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ccf'):
            def ccf(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('dados_bancarios'):
            def dados_bancarios(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('historico_trabalho'):
            def historico_trabalho(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('registro'):
            def rede_vigiada(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ppe'):
            def ppe(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('tcu'):
            def tcu(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ceis'):
            def ceis(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('cepim'):
            def cepim(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('mte'):
            def mte(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('parentesco'):
            def parentesco(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('beneficio_inss'):
            def beneficio_inss(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('servidor_federal'):
            def servidor_federal(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ceaf'):
            def ceaf(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('possivel_morador'):
            def possivel_morador(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('email'):
            def email(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ppe_parentesco'):
            def ppe_parentesco(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('endereco'):
            def endereco(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ancine'):
            def ancine(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('antt_empresas'):
            def antt_empresas(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ans'):
            def ans(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('anvisa'):
            def anvisa(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('rab_anac'):
            def rab_anac(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('cnes'):
            def cnes(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('confea'):
            def confea(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('coren'):
            def coren(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('siccau'):
            def siccau(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('crosp'):
            def crosp(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('cfmv'):
            def cfmv(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ccf'):
            def cfc(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('telefone_fraude'):
            def telefone_fraude(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('anp'):
            def anp(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('prestacao_contas_doador'):
            def ppe_prestador_contas_doador(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('numero_consulta'):
            def numero_consulta(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)

        elif assunto == ('ppe_bens'):
            def ppe_bens(self, diretorio, arquivo):
                return ArquivosDadosIO._spark.read \
                    .format('com.databricks.spark.csv') \
                    .option('header', 'true') \
                    .option('delimiter', ';') \
                    .option('inferSchema', 'true') \
                    .load('/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(
                    str(datetime.today().strftime('%Y%m%d'))) + '/' + diretorio + '/' + arquivo)
        else:
            print('Nao existe essa opcao' + assunto)
            ex = ('Nao existe essa opcao:' + assunto)
            df3 = 'ERRO: {}'.format(str(ex))


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

        def gravar_parquet(self, df, nome, url):
            print('gravando')
            df.write \
                .option("encoding", "UTF-8") \
                .mode("overwrite") \
                .format("parquet") \
                .option("header", "false") \
                .partitionBy('dt_carga').saveAsTable(nome)


    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        # Dataframes originais
        _parametros = None
        

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            print('inicio processo')
            if assunto == ('beneficio_inss'):
                diretorio = 'consta_beneficio'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._beneficio_inss = _dados_io.beneficio_inss(diretorio, 'arquivo_consta_beneficio_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ceaf'):
                diretorio = 'consta_ceaf'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ceaf = _dados_io.ceaf(diretorio, 'arquivo_consta_ceaf_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ceis'):
                diretorio = 'consta_ceis'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ceis = _dados_io.ceis(diretorio, 'arquivo_consta_ceis_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cepim'):
                diretorio = 'consta_cepim'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._cepim = _dados_io.cepim(diretorio, 'arquivo_consta_cepim_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('email'):
                diretorio = 'consta_email'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._email = _dados_io.email(diretorio, 'arquivo_consta_email_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('endereco'):
                diretorio = 'consta_endereco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._endereco = _dados_io.endereco(diretorio, 'arquivo_consta_endereco_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('mte'):
                diretorio = 'consta_mte'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._mte = _dados_io.mte(diretorio, 'arquivo_consta_mte_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('obitos'):
                diretorio = 'consta_obito'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._obitos = _dados_io.obitos(diretorio, 'arquivo_consta_obito_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('parentesco'):
                diretorio = 'consta_parentesco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._parentesco = _dados_io.parentesco(diretorio, 'arquivo_consta_parentesco_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('possivel_morador'):
                diretorio = 'consta_possivel_morador'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._possivel_morador = _dados_io.possivel_morador(diretorio,'arquivo_consta_possivel_morador_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe'):
                diretorio = 'consta_ppe'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ppe = _dados_io.ppe(diretorio, 'arquivo_consta_ppe_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe_bens'):
                diretorio = 'consta_ppebens'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ppe_bens = _dados_io.ppe_bens(diretorio, 'arquivo_consta_ppebens_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe_parentesco'):
                diretorio = 'consta_ppeparentesco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ppe_parentesco = _dados_io.ppe_parentesco(diretorio, 'arquivo_consta_ppeparentesco_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('prestacao_contas_doador'):
                diretorio = 'consta_ppePrestacaoContasDoador'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ppe_prestador_contas_doador = _dados_io.ppe_prestador_contas_doador(diretorio,'arquivo_consta_ppePrestacaoContasDoador_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('servidor_federal'):
                diretorio = 'consta_servidor_federal'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._servidor_federal = _dados_io.servidor_federal(diretorio,'arquivo_consta_servidor_federal_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('tcu'):
                diretorio = 'consta_tcu'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._tcu = _dados_io.tcu(diretorio, 'arquivo_consta_tcu_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('dados_cadastrais'):
                diretorio = 'dadosCadastrais'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._dados_cadastrais = _dados_io.dados_cadastrais(diretorio, 'arquivo_dadosCadastrais_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('empresas'):
                diretorio = 'empresas'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._empresas = _dados_io.empresas(diretorio, 'arquivo_empresas_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ancine'):
                diretorio = 'novo_ancine'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ancine = _dados_io.ancine(diretorio, 'arquivo_novo_ancine_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('anp'):
                diretorio = 'novo_anp'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._anp = _dados_io.anp(diretorio, 'arquivo_novo_anp_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ans'):
                diretorio = 'novo_ans'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ans = _dados_io.ans(diretorio, 'arquivo_novo_ans_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('antt_empresas'):
                diretorio = 'novo_anttEmpresas'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._antt_empresas = _dados_io.antt_empresas(diretorio, 'arquivo_novo_anttEmpresas_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('anvisa'):
                diretorio = 'novo_anvisa'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._anvisa = _dados_io.anvisa(diretorio, 'arquivo_novo_anvisa_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ccf'):
                diretorio = 'novo_ccf'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._ccf = _dados_io.ccf(diretorio, 'arquivo_novo_ccf_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cfc'):
                diretorio = 'novo_cfc'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._cfc = _dados_io.cfc(diretorio, 'arquivo_novo_cfc_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cfmv'):
                diretorio = 'novo_cfmv'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._cfmv = _dados_io.cfmv(diretorio, 'arquivo_novo_cfmv_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cnes'):
                diretorio = 'novo_cnes'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._cnes = _dados_io.cnes(diretorio, 'arquivo_novo_cnes_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('confea'):
                diretorio = 'novo_confea'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._confea = _dados_io.confea(diretorio, 'arquivo_novo_confea_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('coren'):
                diretorio = 'novo_coren'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._coren = _dados_io.coren(diretorio, 'arquivo_novo_coren_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('crosp'):
                diretorio = 'novo_crosp'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._crosp = _dados_io.crosp(diretorio, 'arquivo_novo_crosp_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('dados_bancarios'):
                diretorio = 'novo_dadosBancarios'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._dados_bancarios = _dados_io.dados_bancarios(diretorio,'arquivo_novo_dadosBancarios_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('historico_trabalho'):
                diretorio = 'novo_historicoTrabalho'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._historico_trabalho = _dados_io.historico_trabalho(diretorio,'arquivo_novo_historicoTrabalho_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('rab_anac'):
                diretorio = 'novo_rabAnac'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._rab_anac = _dados_io.rab_anac(diretorio, 'arquivo_novo_rabAnac_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('siccau'):
                diretorio = 'novo_siccau'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._siccau = _dados_io.siccau(diretorio, 'arquivo_novo_siccau_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('sociedades'):
                diretorio = 'novo_socios'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._sociedades = _dados_io.sociedades(diretorio, 'arquivo_novo_socios_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('telefone_fraude'):
                diretorio = 'novo_telefone_fraude'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._telefone_fraude = _dados_io.telefone_fraude(diretorio,'arquivo_novo_telefone_fraude_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('veiculos'):
                diretorio = 'novo_veiculo'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._veiculos = _dados_io.veiculos(diretorio, 'arquivo_novo_veiculo_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('telefone'):
                diretorio = 'novoTelefone'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._telefone = _dados_io.telefone(diretorio, 'arquivo_novo_telefone_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('numero_consulta'):
                diretorio = 'numeroConsulta'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._numero_consulta = _dados_io.numero_consulta(diretorio, 'arquivo_numeroConsulta_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('registro'):
                diretorio = 'registro'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    self._rede_vigiada = _dados_io.rede_vigiada(diretorio, 'arquivo_registro_final.csv')
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            else:
                print('Nao existe essa opcao: ' + assunto)
                ex = ('Nao existe essa opcao: ' + assunto)
                df3 = 'ERRO: {}'.format(str(ex))

        def gera_regras(self):

            if assunto == ('dados_cadastrais'):
                diretorio = 'dadosCadastrais'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou dados cadastrais')
                    resultado = dados_cadastrais(self._dados_cadastrais)
                    tabela_hive_gravar = 'harmonized.h_all_check_dados_cadastrais'
                    url = '/hive/warehouse/harmonized.db/h_all_check_dados_cadastrais/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('obitos'):
                diretorio = 'consta_obito'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou obitos')
                    resultado = obitos(self._obitos)
                    tabela_hive_gravar = 'harmonized.h_all_check_obitos'
                    url = '/hive/warehouse/harmonized.db/h_all_check_obitos/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('telefone'):
                diretorio = 'novoTelefone'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou telefones')
                    resultado = telefone(self._telefone)
                    tabela_hive_gravar = 'harmonized.h_all_check_telefone'
                    url = '/hive/warehouse/harmonized.db/h_all_check_telefone/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('empresas'):
                diretorio = 'empresas'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou empresas')
                    resultado = empresas(self._empresas)
                    tabela_hive_gravar = 'harmonized.h_all_check_empresas'
                    url = '/hive/warehouse/harmonized.db/h_all_check_empresas/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('veiculos'):
                diretorio = 'novo_veiculo'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou veiculos')
                    resultado = veiculos(self._veiculos)
                    tabela_hive_gravar = 'harmonized.h_all_check_veiculos'
                    url = '/hive/warehouse/harmonized.db/h_all_check_veiculos/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ccf'):
                diretorio = 'novo_ccf'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ccf')
                    resultado = ccf(self._ccf)
                    tabela_hive_gravar = 'harmonized.h_all_check_ccf'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ccf/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('dados_bancarios'):
                diretorio = 'novo_dadosBancarios'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou dados bancarios')
                    resultado = dados_bancarios(self._dados_bancarios)
                    tabela_hive_gravar = 'harmonized.h_all_check_dados_bancarios'
                    url = '/hive/warehouse/harmonized.db/h_all_check_dados_bancarios/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('historico_trabalho'):
                diretorio = 'novo_historicoTrabalho'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou historico_trabalho')
                    resultado = historico_trabalho(self._historico_trabalho)
                    tabela_hive_gravar = 'harmonized.h_all_check_historico_trabalho'
                    url = '/hive/warehouse/harmonized.db/h_all_check_historico_trabalho/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('registro'):
                diretorio = 'registro'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou rede vigiada')
                    resultado = rede_vigiada(self._rede_vigiada)
                    tabela_hive_gravar = 'harmonized.h_all_check_rede_vigiada'
                    url = '/hive/warehouse/harmonized.db/h_all_check_rede_vigiada/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe'):
                diretorio = 'consta_ppe'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ppe')
                    resultado = ppe(self._ppe)
                    tabela_hive_gravar = 'harmonized.h_all_check_ppe'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ppe/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('tcu'):
                diretorio = 'consta_tcu'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou tcu')
                    resultado = tcu(self._tcu)
                    tabela_hive_gravar = 'harmonized.h_all_check_tcu'
                    url = '/hive/warehouse/harmonized.db/h_all_check_tcu/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ceis'):
                diretorio = 'consta_ceis'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ceis')
                    resultado = ceis(self._ceis)
                    tabela_hive_gravar = 'harmonized.h_all_check_ceis'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ceis/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cepim'):
                diretorio = 'consta_cepim'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou cepim')
                    resultado = cepim(self._cepim)
                    tabela_hive_gravar = 'harmonized.h_all_check_cepim'
                    url = '/hive/warehouse/harmonized.db/h_all_check_cepim/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('mte'):
                diretorio = 'consta_mte'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou mte')
                    resultado = mte(self._mte)
                    tabela_hive_gravar = 'harmonized.h_all_check_mte'
                    url = '/hive/warehouse/harmonized.db/h_all_check_mte/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe_parentesco'):
                diretorio = 'consta_ppeparentesco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou parentesco')
                    resultado = ppe_parentesco(self._ppe_parentesco)
                    tabela_hive_gravar = 'harmonized.h_all_check_ppe_parentesco'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ppe_parentesco/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('beneficio_inss'):
                diretorio = 'consta_beneficio'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou beneficio_inss')
                    resultado = beneficio_inss(self._beneficio_inss)
                    tabela_hive_gravar = 'harmonized.h_all_check_beneficio_inss'
                    url = '/hive/warehouse/harmonized.db/h_all_check_beneficio_inss/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('servidor_federal'):
                diretorio = 'consta_servidor_federal'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou servidor_federal')
                    resultado = servidor_federal(self._servidor_federal)
                    tabela_hive_gravar = 'harmonized.h_all_check_servidor_federal'
                    url = '/hive/warehouse/harmonized.db/h_all_check_servidor_federal/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ceaf'):
                diretorio = 'consta_ceaf'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ceaf')
                    resultado = ceaf(self._ceaf)
                    tabela_hive_gravar = 'harmonized.h_all_check_ceaf'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ceaf/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('possivel_morador'):
                diretorio = 'consta_possivel_morador'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou possivel_morador')
                    resultado = possivel_morador(self._possivel_morador)
                    tabela_hive_gravar = 'harmonized.h_all_check_possivel_morador'
                    url = '/hive/warehouse/harmonized.db/h_all_check_possivel_morador/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('email'):
                diretorio = 'consta_email'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou email')
                    resultado = email(self._email)
                    tabela_hive_gravar = 'harmonized.h_all_check_email'
                    url = '/hive/warehouse/harmonized.db/h_all_check_email/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('parentesco'):
                diretorio = 'consta_parentesco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou parentesco')
                    resultado = parentesco(self._parentesco)
                    tabela_hive_gravar = 'harmonized.h_all_check_parentesco'
                    url = '/hive/warehouse/harmonized.db/h_all_check_parentesco/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('endereco'):
                diretorio = 'consta_endereco'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou endereco')
                    resultado = endereco(self._endereco)
                    tabela_hive_gravar = 'harmonized.h_all_check_endereco'
                    url = '/hive/warehouse/harmonized.db/h_all_check_endereco/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ancine'):
                diretorio = 'novo_ancine'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ancine')
                    resultado = ancine(self._ancine)
                    tabela_hive_gravar = 'harmonized.h_all_check_ancine'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ancine/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('antt_empresas'):
                diretorio = 'novo_anttEmpresas'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou antt_empresas')
                    resultado = antt_empresas(self._antt_empresas)
                    tabela_hive_gravar = 'harmonized.h_all_check_antt_empresas'
                    url = '/hive/warehouse/harmonized.db/h_all_check_antt_empresas/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ans'):
                diretorio = 'novo_ans'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ans')
                    resultado = ans(self._ans)
                    tabela_hive_gravar = 'harmonized.h_all_check_ans'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ans/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('anvisa'):
                diretorio = 'novo_anvisa'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou anvisa')
                    resultado = anvisa(self._anvisa)
                    tabela_hive_gravar = 'harmonized.h_all_check_anvisa'
                    url = '/hive/warehouse/harmonized.db/h_all_check_anvisa/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('rab_anac'):
                diretorio = 'novo_rabAnac'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou rab_anac')
                    resultado = rab_anac(self._rab_anac)
                    tabela_hive_gravar = 'harmonized.h_all_check_rab_anac'
                    url = '/hive/warehouse/harmonized.db/h_all_check_rab_anac/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cnes'):
                diretorio = 'novo_cnes'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou cnes')
                    resultado = cnes(self._cnes)
                    tabela_hive_gravar = 'harmonized.h_all_check_cnes'
                    url = '/hive/warehouse/harmonized.db/h_all_check_cnes/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('confea'):
                diretorio = 'novo_confea'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou confea')
                    resultado = confea(self._confea)
                    tabela_hive_gravar = 'harmonized.h_all_check_confea'
                    url = '/hive/warehouse/harmonized.db/h_all_check_confea/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('coren'):
                diretorio = 'novo_coren'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou coren')
                    resultado = coren(self._coren)
                    tabela_hive_gravar = 'harmonized.h_all_check_coren'
                    url = '/hive/warehouse/harmonized.db/h_all_check_coren/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('siccau'):
                diretorio = 'novo_siccau'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou siccau')
                    resultado = siccau(self._siccau)
                    tabela_hive_gravar = 'harmonized.h_all_check_siccau'
                    url = '/hive/warehouse/harmonized.db/h_all_check_siccau/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('crosp'):
                diretorio = 'novo_crosp'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou crosp')
                    resultado = crosp(self._crosp)
                    tabela_hive_gravar = 'harmonized.h_all_check_crosp'
                    url = '/hive/warehouse/harmonized.db/h_all_check_crosp/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cfmv'):
                diretorio = 'novo_cfmv'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou cfmv')
                    resultado = cfmv(self._cfmv)
                    tabela_hive_gravar = 'harmonized.h_all_check_cfmv'
                    url = '/hive/warehouse/harmonized.db/h_all_check_cfmv/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('cfc'):
                diretorio = 'novo_cfc'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou cfc')
                    resultado = cfc(self._cfc)
                    tabela_hive_gravar = 'harmonized.h_all_check_cfc'
                    url = '/hive/warehouse/harmonized.db/h_all_check_cfc/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('telefone_fraude'):
                diretorio = 'novo_telefone_fraude'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou telefone_fraude')
                    resultado = telefone_fraude(self._telefone_fraude)
                    tabela_hive_gravar = 'harmonized.h_all_check_telefone_fraude'
                    url = '/hive/warehouse/harmonized.db/h_all_check_telefone_fraude/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('anp'):
                diretorio = 'novo_anp'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou anp')
                    resultado = anp(self._anp)
                    tabela_hive_gravar = 'harmonized.h_all_check_anp'
                    url = '/hive/warehouse/harmonized.db/h_all_check_anp/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('prestacao_contas_doador'):
                diretorio = 'consta_ppePrestacaoContasDoador'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou prestacao_contas_doador')
                    resultado = ppe_prestador_contas_doador(self._ppe_prestador_contas_doador)
                    tabela_hive_gravar = 'harmonized.h_all_check_ppe_prest_contas_doador'
                    url = '/hive/warehouse/harmonized.db/h_all_check_prestacao_contas_doador/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('ppe_bens'):
                diretorio = 'consta_ppebens'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou ppe_bens')
                    resultado = ppe_bens(self._ppe_bens)
                    tabela_hive_gravar = 'harmonized.h_all_check_ppe_bens'
                    url = '/hive/warehouse/harmonized.db/h_all_check_ppe_bens/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('numero_consulta'):
                diretorio = 'numeroConsulta'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou numero_consulta')
                    resultado = numero_consulta(self._numero_consulta)
                    tabela_hive_gravar = 'harmonized.h_all_check_numero_consulta'
                    url = '/hive/warehouse/harmonized.db/h_all_check_numero_consulta/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            elif assunto == ('sociedades'):
                diretorio = 'novo_socios'
                diretorio_final = '/data/raw/FICO/AllCheckAnaliseFullOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))) + diretorio + '/'
                if valida_diretorio(diretorio_final) == True:
                    print('entrou sociedades')
                    resultado = sociedades(self._sociedades)
                    tabela_hive_gravar = 'harmonized.h_all_check_sociedades'
                    url = '/hive/warehouse/harmonized.db/h_all_check_sociedades/'
                    _dadosTabela_io.gravar_parquet(resultado, tabela_hive_gravar, url)
                else:
                    print('Nao existe o diretorio')
                    exit(0)
            else:
                print ('Diretorio nao encontrado para o assunto: ' + assunto)
                ex = ('Diretorio nao encontrado para o assunto: ' + assunto)
                df3 = 'ERRO: {}'.format(str(ex))
                resultado = None
                exit(0)

            return resultado


    def dados_cadastrais(df_dados_cadastrais):
        return (df_dados_cadastrais
                .select(df_dados_cadastrais['cpf'].cast(StringType()).alias('no_cpf'),
                        df_dados_cadastrais['telefone'].cast(StringType()).alias('no_telefone'),
                        df_dados_cadastrais['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_dados_cadastrais['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_dados_cadastrais['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_dados_cadastrais['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_dados_cadastrais['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_dados_cadastrais['numeroConsulta'].cast(StringType()).alias('no_consulta'),
                        df_dados_cadastrais['nome'].cast(StringType()).alias('nm_cliente'),
                        df_dados_cadastrais['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_dados_cadastrais['rg'].cast(StringType()).alias('no_rg'),
                        df_dados_cadastrais['dataNascimento'].cast(StringType()).alias('dt_nascimento'),
                        df_dados_cadastrais['signo'].cast(StringType()).alias('ds_signo'),
                        df_dados_cadastrais['email'].cast(StringType()).alias('ds_email'),
                        df_dados_cadastrais['filiacao'].cast(StringType()).alias('ds_filiacao'),
                        df_dados_cadastrais['pis'].cast(StringType()).alias('no_pis'),
                        df_dados_cadastrais['carteiraProf'].cast(StringType()).alias('no_carteira_profissional'),
                        df_dados_cadastrais['cnpjEmpregador'].cast(StringType()).alias('no_cnpj_empregador'),
                        df_dados_cadastrais['dataAdmissao'].cast(StringType()).alias('dt_admissao'),
                        df_dados_cadastrais['atividade'].cast(StringType()).alias('ds_atividade'),
                        df_dados_cadastrais['grauInstrucao'].cast(StringType()).alias('ds_grau_instrucao'),
                        df_dados_cadastrais['sexo'].cast(StringType()).alias('ds_sexo'),
                        df_dados_cadastrais['estadoCivil'].cast(StringType()).alias('ds_estado_civil'),
                        df_dados_cadastrais['numeroBeneficio'].cast(StringType()).alias('no_beneficio'),
                        df_dados_cadastrais['valorBeneficio'].cast(StringType()).alias('vl_beneficio'),
                        df_dados_cadastrais['inicioBeneficio'].cast(StringType()).alias('dt_inicio_beneficio'),
                        df_dados_cadastrais['numeroTitulo'].cast(StringType()).alias('no_titulo'),
                        df_dados_cadastrais['nacionalidade'].cast(StringType()).alias('ds_nacionalidade')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def obitos(df_obitos):
        return (df_obitos
                .select(df_obitos['cpf'].cast(StringType()).alias('no_cpf'),
                        df_obitos['telefone'].cast(StringType()).alias('no_telefone'),
                        df_obitos['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_obitos['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_obitos['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_obitos['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_obitos['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_obitos['nome'].cast(StringType()).alias('nm_obito'),
                        df_obitos['nomeMae'].cast(StringType()).alias('nm_mae'),
                        df_obitos['dataObito'].cast(StringType()).alias('dt_obito'),
                        df_obitos['dataNascimento'].cast(StringType()).alias('dt_nascimento')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def sociedades(df_sociedades):
        return (df_sociedades
                .select(df_sociedades['cpf'].cast(StringType()).alias('no_cpf'),
                        df_sociedades['telefone'].cast(StringType()).alias('no_telefone'),
                        df_sociedades['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_sociedades['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_sociedades['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_sociedades['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_sociedades['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_sociedades['socio'].cast(StringType()).alias('nm_socio'),
                        df_sociedades['cpf_socio'].cast(StringType()).alias('no_cpf_socio'),
                        df_sociedades['rg_socio'].cast(StringType()).alias('cd_rg_socio'),
                        df_sociedades['endereco_socio'].cast(StringType()).alias('ds_endereco_socio'),
                        df_sociedades['numero_socio'].cast(StringType()).alias('no_endereco_socio'),
                        df_sociedades['complemento_socio'].cast(StringType()).alias('ds_complemento_socio'),
                        df_sociedades['bairro_socio'].cast(StringType()).alias('ds_bairro_socio'),
                        df_sociedades['estado_socio'].cast(StringType()).alias('ds_estado_socio'),
                        df_sociedades['cep_socio'].cast(StringType()).alias('cd_cep_socio'),
                        df_sociedades['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_sociedades['inscricao_estadual'].cast(StringType()).alias('no_inscricao_estadual'),
                        df_sociedades['participacao'].cast(StringType()).alias('ds_participacao'),
                        df_sociedades['data_ingresso'].cast(StringType()).alias('dt_ingresso'),
                        df_sociedades['data_inicio'].cast(StringType()).alias('dt_inicio'),
                        df_sociedades['nome'].cast(StringType()).alias('nm_empresa_socio'),
                        df_sociedades['endereco_empresa'].cast(StringType()).alias('ds_endereco_empresa_socio'),
                        df_sociedades['numero_empresa'].cast(StringType()).alias('no_endereco_empresa_socio'),
                        df_sociedades['complemento_empresa'].cast(StringType()).alias('ds_complemento_empresa_socio'),
                        df_sociedades['bairro_empresa'].cast(StringType()).alias('ds_bairro_empresa'),
                        df_sociedades['estado_empresa'].cast(StringType()).alias('ds_estado_empresa_socio'),
                        df_sociedades['cep_empresa'].cast(StringType()).alias('cd_cep_empresa_socio'),
                        df_sociedades['socios_da_empresa'].cast(StringType()).alias('nm_socios_da_empresa')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def telefone(df_telefone):
        return (df_telefone
                .select(df_telefone['cpf'].cast(StringType()).alias('no_cpf'),
                        df_telefone['telefone1'].cast(StringType()).alias('no_telefone'),
                        df_telefone['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_telefone['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_telefone['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_telefone['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_telefone['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_telefone['nome'].cast(StringType()).alias('nm_telefone'),
                        df_telefone['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_telefone['ddd'].cast(StringType()).alias('no_ddd'),
                        df_telefone['telefone10'].cast(StringType()).alias('no_telefone_contato'),
                        df_telefone['endereco'].cast(StringType()).alias('ds_endereco'),
                        df_telefone['numero'].cast(StringType()).alias('no_endereco'),
                        df_telefone['cep'].cast(StringType()).alias('cd_cep'),
                        df_telefone['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_telefone['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_telefone['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_telefone['estado'].cast(StringType()).alias('ds_estado'),
                        df_telefone['dataInstalacao'].cast(StringType()).alias('dt_instalacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def empresas(df_empresas):
        return (df_empresas
                .select(df_empresas['cpf'].cast(StringType()).alias('no_cpf'),
                        df_empresas['telefone'].cast(StringType()).alias('no_telefone'),
                        df_empresas['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_empresas['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_empresas['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_empresas['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_empresas['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_empresas['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_empresas['nome'].cast(StringType()).alias('nm_empresa'),
                        df_empresas['fantasia'].cast(StringType()).alias('nm_fantasia'),
                        df_empresas['data_abertura'].cast(StringType()).alias('dt_abertura'),
                        df_empresas['atividade_comercial'].cast(StringType()).alias('ds_atividade_comercial'),
                        df_empresas['cnae'].cast(StringType()).alias('cd_cnae'),
                        df_empresas['cnae2'].cast(StringType()).alias('cd_cnae2'),
                        df_empresas['natureza_juridica'].cast(StringType()).alias('ds_natureza_juridica'),
                        df_empresas['endereco'].cast(StringType()).alias('ds_endereco'),
                        df_empresas['numero'].cast(StringType()).alias('no_endereco'),
                        df_empresas['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_empresas['cep'].cast(StringType()).alias('cd_cep'),
                        df_empresas['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_empresas['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_empresas['estado'].cast(StringType()).alias('ds_estado'),
                        df_empresas['situacao_cadastral'].cast(StringType()).alias('ds_situacao_cadastral'),
                        df_empresas['porte'].cast(StringType()).alias('ds_porte'),
                        df_empresas['tipo'].cast(StringType()).alias('tp_empresa'),
                        df_empresas['dataSituacao_cadastral'].cast(StringType()).alias('dt_situacao_cadastral'),
                        df_empresas['situacao_especial'].cast(StringType()).alias('ds_situacao_especial'),
                        df_empresas['dataSituacao_especial'].cast(StringType()).alias('dt_situacao_especial')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def veiculos(df_veiculos):
        return (df_veiculos
                .select(df_veiculos['cpf'].cast(StringType()).alias('no_cpf'),
                        df_veiculos['telefone'].cast(StringType()).alias('no_telefone'),
                        df_veiculos['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_veiculos['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_veiculos['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_veiculos['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_veiculos['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_veiculos['nome'].cast(StringType()).alias('nm_cliente'),
                        df_veiculos['cpf_cnpj'].cast(StringType()).alias('no_cpf_cnpj'),
                        df_veiculos['placa'].cast(StringType()).alias('no_placa'),
                        df_veiculos['renavam'].cast(StringType()).alias('no_renavam'),
                        df_veiculos['marca'].cast(StringType()).alias('ds_marca'),
                        df_veiculos['combustivel'].cast(StringType()).alias('ds_combustivel'),
                        df_veiculos['ano_fabricacao'].cast(StringType()).alias('aa_fabricacao'),
                        df_veiculos['chassis'].cast(StringType()).alias('no_chassis'),
                        df_veiculos['endereco'].cast(StringType()).alias('ds_endereco'),
                        df_veiculos['numero'].cast(StringType()).alias('no_endereco'),
                        df_veiculos['cep'].cast(StringType()).alias('cd_cep'),
                        df_veiculos['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_veiculos['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_veiculos['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_veiculos['uf'].cast(StringType()).alias('ds_estado'),
                        df_veiculos['ddd'].cast(StringType()).alias('no_ddd'),
                        df_veiculos['fone'].cast(StringType()).alias('no_telefone_2'),
                        df_veiculos['ddd1'].cast(StringType()).alias('no_ddd_1'),
                        df_veiculos['fone1'].cast(StringType()).alias('no_telefone_1'),
                        df_veiculos['data_movimento'].cast(StringType()).alias('dt_movimento'),
                        df_veiculos['ano_base'].cast(StringType()).alias('aa_base')
                        )

                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ccf(df_ccf):
        return (df_ccf
                .select(df_ccf['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ccf['telefone1'].cast(StringType()).alias('no_telefone'),
                        df_ccf['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ccf['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ccf['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ccf['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ccf['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ccf['nome'].cast(StringType()).alias('nm_ccf'),
                        df_ccf['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_ccf['quantidadeDeOcorrencias'].cast(StringType()).alias('qt_ocorrencias'),
                        df_ccf['nomeBanco'].cast(StringType()).alias('nm_banco'),
                        df_ccf['nomeAgencia'].cast(StringType()).alias('nm_agencia'),
                        df_ccf['enderecoAgencia'].cast(StringType()).alias('ds_endereco_agencia'),
                        df_ccf['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_ccf['estado'].cast(StringType()).alias('ds_estado'),
                        df_ccf['cep'].cast(StringType()).alias('cd_cep'),
                        df_ccf['ddd'].cast(StringType()).alias('no_ddd'),
                        df_ccf['telefone17'].cast(StringType()).alias('no_telefone_1'),
                        df_ccf['fax'].cast(StringType()).alias('no_fax'),
                        df_ccf['banco'].cast(StringType()).alias('no_banco'),
                        df_ccf['agencia'].cast(StringType()).alias('no_agencia'),
                        df_ccf['motivoDevolucao'].cast(StringType()).alias('ds_motivo_devolucao'),
                        df_ccf['dataDaUltimaOcorrencia'].cast(StringType()).alias('dt_ultima_ocorrencia')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def dados_bancarios(df_dados_bancarios):
        return (df_dados_bancarios
                .select(df_dados_bancarios['cpf'].cast(StringType()).alias('no_cpf'),
                        df_dados_bancarios['telefone1'].cast(StringType()).alias('no_telefone'),
                        df_dados_bancarios['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_dados_bancarios['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_dados_bancarios['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_dados_bancarios['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_dados_bancarios['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_dados_bancarios['nomeBanco'].cast(StringType()).alias('nm_banco'),
                        df_dados_bancarios['nomeAgencia'].cast(StringType()).alias('nm_agencia'),
                        df_dados_bancarios['enderecoAgencia'].cast(StringType()).alias('ds_endereco_agencia'),
                        df_dados_bancarios['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_dados_bancarios['estado'].cast(StringType()).alias('ds_estado'),
                        df_dados_bancarios['cep'].cast(StringType()).alias('cd_cep'),
                        df_dados_bancarios['ddd'].cast(StringType()).alias('no_ddd'),
                        df_dados_bancarios['telefone14'].cast(StringType()).alias('no_telefone_1'),
                        df_dados_bancarios['fax'].cast(StringType()).alias('no_fax'),
                        df_dados_bancarios['banco'].cast(StringType()).alias('no_banco'),
                        df_dados_bancarios['agencia'].cast(StringType()).alias('no_agencia'),
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def historico_trabalho(df_historico_trabalho):
        return (df_historico_trabalho
                .select(df_historico_trabalho['cpf0'].cast(StringType()).alias('no_cpf'),
                        df_historico_trabalho['telefone'].cast(StringType()).alias('no_telefone'),
                        df_historico_trabalho['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_historico_trabalho['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_historico_trabalho['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_historico_trabalho['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_historico_trabalho['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_historico_trabalho['dataAdmissao'].cast(StringType()).alias('dt_admissao'),
                        df_historico_trabalho['nome'].cast(StringType()).alias('nm_cliente'),
                        df_historico_trabalho['cpf9'].cast(StringType()).alias('no_cpf_1'),
                        df_historico_trabalho['fre'].cast(StringType()).alias('ds_fre'),
                        df_historico_trabalho['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_historico_trabalho['nomeEmpresa'].cast(StringType()).alias('nm_empresa')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def rede_vigiada(df_rede_vigiada):
        return (df_rede_vigiada
                .select(df_rede_vigiada['cpf'].cast(StringType()).alias('no_cpf'),
                        df_rede_vigiada['telefone1'].cast(StringType()).alias('no_telefone'),
                        df_rede_vigiada['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_rede_vigiada['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_rede_vigiada['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_rede_vigiada['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_rede_vigiada['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_rede_vigiada['dataConsulta'].cast(StringType()).alias('dt_consulta'),
                        df_rede_vigiada['produto'].cast(StringType()).alias('ds_produto'),
                        df_rede_vigiada['cliente'].cast(StringType()).alias('nm_cliente'),
                        df_rede_vigiada['telefone10'].cast(StringType()).alias('no_telefone_1')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ppe(df_ppe):
        return (df_ppe
                .select(df_ppe['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ppe['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ppe['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ppe['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ppe['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ppe['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ppe['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ppe['status'].cast(StringType()).alias('ds_status'),
                        df_ppe['nome'].cast(StringType()).alias('nm_ppe'),
                        df_ppe['data_nascimento'].cast(StringType()).alias('dt_nascimento'),
                        df_ppe['numero_titulo'].cast(StringType()).alias('no_titulo'),
                        df_ppe['grau_instrucao'].cast(StringType()).alias('ds_grau_instrucao'),
                        df_ppe['estado_civil'].cast(StringType()).alias('ds_estado_civil'),
                        df_ppe['nacionalidade'].cast(StringType()).alias('ds_nacionalidade'),
                        df_ppe['municipio_nascimento'].cast(StringType()).alias('nm_cidade_nascimento'),
                        df_ppe['uf_nascimento'].cast(StringType()).alias('cd_estado_nascimento'),
                        df_ppe['descricao_ocupacao'].cast(StringType()).alias('ds_ocupacao'),
                        df_ppe['nome_urna'].cast(StringType()).alias('nm_urna'),
                        df_ppe['numero_candidato'].cast(StringType()).alias('no_candidato'),
                        df_ppe['numero_partido'].cast(StringType()).alias('no_partido'),
                        df_ppe['sigla_partido'].cast(StringType()).alias('ds_sigla_partido'),
                        df_ppe['nome_partido'].cast(StringType()).alias('nm_partido'),
                        df_ppe['sigla_uf'].cast(StringType()).alias('cd_estado'),
                        df_ppe['municipio'].cast(StringType()).alias('nm_cidade'),
                        df_ppe['cargo'].cast(StringType()).alias('nm_cargo'),
                        df_ppe['poder'].cast(StringType()).alias('ds_poder'),
                        df_ppe['instituicao'].cast(StringType()).alias('ds_instituicao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def tcu(df_tcu):
        return (df_tcu
                .select(df_tcu['cpf'].cast(StringType()).alias('no_cpf'),
                        df_tcu['telefone'].cast(StringType()).alias('no_telefone'),
                        df_tcu['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_tcu['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_tcu['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_tcu['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_tcu['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_tcu['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_tcu['nome'].cast(StringType()).alias('nm_tcu'),
                        df_tcu['processo'].cast(StringType()).alias('no_processo'),
                        df_tcu['deliberacao'].cast(StringType()).alias('ds_deliberacao'),
                        df_tcu['estado'].cast(StringType()).alias('cd_estado'),
                        df_tcu['data_julgamento'].cast(StringType()).alias('dt_julgamento')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ceis(df_ceis):
        return (df_ceis
                .select(df_ceis['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ceis['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ceis['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ceis['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ceis['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ceis['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ceis['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ceis['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_ceis['nome'].cast(StringType()).alias('nm_ceis'),
                        df_ceis['numero_processo'].cast(StringType()).alias('no_processo'),
                        df_ceis['tipo_sansao'].cast(StringType()).alias('tp_sansao'),
                        df_ceis['data_inicio'].cast(StringType()).alias('dt_inicio'),
                        df_ceis['data_final'].cast(StringType()).alias('dt_final')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def cepim(df_cepim):
        return (df_cepim
                .select(df_cepim['cpf'].cast(StringType()).alias('no_cpf'),
                        df_cepim['telefone'].cast(StringType()).alias('no_telefone'),
                        df_cepim['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_cepim['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_cepim['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_cepim['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_cepim['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_cepim['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_cepim['nome'].cast(StringType()).alias('nm_cliente'),
                        df_cepim['num_convenio'].cast(StringType()).alias('no_convenio'),
                        df_cepim['orgao_concedente'].cast(StringType()).alias('ds_orgao_concedente'),
                        df_cepim['motivo_impedimento'].cast(StringType()).alias('ds_motivo_impedimento')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def mte(df_mte):
        return (df_mte
                .select(df_mte['cpf'].cast(StringType()).alias('no_cpf'),
                        df_mte['telefone'].cast(StringType()).alias('no_telefone'),
                        df_mte['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_mte['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_mte['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_mte['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_mte['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_mte['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_mte['nome'].cast(StringType()).alias('nm_mte'),
                        df_mte['razao_social'].cast(StringType()).alias('nm_razao_social'),
                        df_mte['ano'].cast(StringType()).alias('aa_mte'),
                        df_mte['municipio'].cast(StringType()).alias('nm_cidade'),
                        df_mte['estado'].cast(StringType()).alias('cd_estado'),
                        df_mte['num_trabalhadores'].cast(StringType()).alias('no_trabalhadores')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def parentesco(df_parentesco):
        return (df_parentesco
                .select(df_parentesco['cpf'].cast(StringType()).alias('no_cpf'),
                        df_parentesco['telefone'].cast(StringType()).alias('no_telefone'),
                        df_parentesco['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_parentesco['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_parentesco['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_parentesco['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_parentesco['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_parentesco['cpf_parente'].cast(StringType()).alias('no_cpf_1'),
                        df_parentesco['nome_parente'].cast(StringType()).alias('nm_parente'),
                        df_parentesco['grau_parentesco'].cast(StringType()).alias('ds_grau_parentesco')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def beneficio_inss(df_beneficio_inss):
        return (df_beneficio_inss
                .select(df_beneficio_inss['cpf'].cast(StringType()).alias('no_cpf'),
                        df_beneficio_inss['telefone'].cast(StringType()).alias('no_telefone'),
                        df_beneficio_inss['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_beneficio_inss['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_beneficio_inss['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_beneficio_inss['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_beneficio_inss['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_beneficio_inss['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_beneficio_inss['nome'].cast(StringType()).alias('nm_beneficiario'),
                        df_beneficio_inss['numeroBeneficio'].cast(StringType()).alias('no_beneficio'),
                        df_beneficio_inss['dataInicioBeneficio'].cast(StringType()).alias('dt_inicio'),
                        df_beneficio_inss['descricaoBeneficio'].cast(StringType()).alias('ds_beneficio')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def servidor_federal(df_servidor_federal):
        return (df_servidor_federal
                .select(df_servidor_federal['cpf'].cast(StringType()).alias('no_cpf'),
                        df_servidor_federal['telefone'].cast(StringType()).alias('no_telefone'),
                        df_servidor_federal['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_servidor_federal['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_servidor_federal['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_servidor_federal['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_servidor_federal['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_servidor_federal['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_servidor_federal['nome'].cast(StringType()).alias('nm_servidor'),
                        df_servidor_federal['descricao_cargo'].cast(StringType()).alias('ds_cargo'),
                        df_servidor_federal['org_lotacao'].cast(StringType()).alias('ds_org_lotacao'),
                        df_servidor_federal['org_exercicio'].cast(StringType()).alias('ds_org_exercicio'),
                        df_servidor_federal['servidor'].cast(StringType()).alias('ds_servidor')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ceaf(df_ceaf):
        return (df_ceaf
                .select(df_ceaf['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ceaf['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ceaf['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ceaf['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ceaf['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ceaf['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ceaf['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ceaf['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_ceaf['nome'].cast(StringType()).alias('nm_ceaf'),
                        df_ceaf['org_lotacao'].cast(StringType()).alias('ds_org_lotacao'),
                        df_ceaf['tipo_punicao'].cast(StringType()).alias('tp_punicao'),
                        df_ceaf['data_public_punicao'].cast(StringType()).alias('dt_publicacao_punicao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def possivel_morador(df_possivel_morador):
        return (df_possivel_morador
                .select(df_possivel_morador['cpf'].cast(StringType()).alias('no_cpf'),
                        df_possivel_morador['telefone'].cast(StringType()).alias('no_telefone'),
                        df_possivel_morador['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_possivel_morador['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_possivel_morador['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_possivel_morador['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_possivel_morador['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_possivel_morador['cpf_morador'].cast(StringType()).alias('no_cpf_morador'),
                        df_possivel_morador['nome_morador'].cast(StringType()).alias('nm_morador')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def email(df_email):
        return (df_email
                .select(df_email['cpf'].cast(StringType()).alias('no_cpf'),
                        df_email['telefone'].cast(StringType()).alias('no_telefone'),
                        df_email['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_email['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_email['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_email['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_email['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_email['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_email['email'].cast(StringType()).alias('ds_email')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ppe_parentesco(df_ppe_parentesco):
        return (df_ppe_parentesco
                .select(df_ppe_parentesco['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ppe_parentesco['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ppe_parentesco['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ppe_parentesco['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ppe_parentesco['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ppe_parentesco['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ppe_parentesco['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ppe_parentesco['cpf_parente'].cast(StringType()).alias('no_cpf_parente'),
                        df_ppe_parentesco['nome_parente'].cast(StringType()).alias('nm_parente'),
                        df_ppe_parentesco['grau_parentesco'].cast(StringType()).alias('ds_grau_parentesco')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def endereco(df_endereco):
        return (df_endereco
                .select(df_endereco['cpf'].cast(StringType()).alias('no_cpf'),
                        df_endereco['telefone'].cast(StringType()).alias('no_telefone'),
                        df_endereco['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_endereco['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_endereco['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_endereco['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_endereco['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_endereco['endereco'].cast(StringType()).alias('ds_endereco'),
                        df_endereco['numero'].cast(StringType()).alias('no_endereco'),
                        df_endereco['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_endereco['cep'].cast(StringType()).alias('cd_cep'),
                        df_endereco['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_endereco['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_endereco['estado'].cast(StringType()).alias('cd_estado')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ancine(df_ancine):
        return (df_ancine
                .select(df_ancine['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ancine['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ancine['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ancine['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ancine['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ancine['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ancine['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ancine['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_ancine['razao'].cast(StringType()).alias('nm_razao_social'),
                        df_ancine['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_ancine['cep'].cast(StringType()).alias('cd_cep'),
                        df_ancine['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_ancine['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_ancine['estado'].cast(StringType()).alias('cd_estado')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def antt_empresas(df_antt_empresas):
        return (df_antt_empresas
                .select(df_antt_empresas['cpf'].cast(StringType()).alias('no_cpf'),
                        df_antt_empresas['telefone'].cast(StringType()).alias('no_telefone'),
                        df_antt_empresas['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_antt_empresas['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_antt_empresas['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_antt_empresas['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_antt_empresas['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_antt_empresas['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_antt_empresas['razao'].cast(StringType()).alias('nm_razao_social'),
                        df_antt_empresas['situacao'].cast(StringType()).alias('ds_situacao'),
                        df_antt_empresas['endereco'].cast(StringType()).alias('ds_endereco')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ans(df_ans):
        return (df_ans
                .select(df_ans['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ans['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ans['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ans['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ans['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ans['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ans['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ans['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_ans['razao'].cast(StringType()).alias('nm_razao_social'),
                        df_ans['fantasia'].cast(StringType()).alias('nm_fantasia'),
                        df_ans['registro'].cast(StringType()).alias('ds_registro'),
                        df_ans['modalidade'].cast(StringType()).alias('ds_modalidade')

                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def anvisa(df_anvisa):
        return (df_anvisa
                .select(df_anvisa['cpf'].cast(StringType()).alias('no_cpf'),
                        df_anvisa['telefone'].cast(StringType()).alias('no_telefone'),
                        df_anvisa['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_anvisa['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_anvisa['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_anvisa['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_anvisa['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_anvisa['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_anvisa['razao'].cast(StringType()).alias('nm_razao_social'),
                        df_anvisa['cidade'].cast(StringType()).alias('nm_cidade'),
                        df_anvisa['estado'].cast(StringType()).alias('cd_estado'),
                        df_anvisa['numero'].cast(StringType()).alias('no_endereco'),
                        df_anvisa['tipo'].cast(StringType()).alias('tp_anvisa'),
                        df_anvisa['situacao'].cast(StringType()).alias('ds_situacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def rab_anac(df_rab_anac):
        return (df_rab_anac
                .select(df_rab_anac['cpf'].cast(StringType()).alias('no_cpf'),
                        df_rab_anac['telefone'].cast(StringType()).alias('no_telefone'),
                        df_rab_anac['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_rab_anac['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_rab_anac['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_rab_anac['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_rab_anac['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_rab_anac['marca'].cast(StringType()).alias('ds_marca'),
                        df_rab_anac['proprietario'].cast(StringType()).alias('nm_proprietario'),
                        df_rab_anac['ufProprietario'].cast(StringType()).alias('cd_estado_proprietario'),
                        df_rab_anac['cpfCnpjProprietario'].cast(StringType()).alias('no_cpf_cnpj_proprietario'),
                        df_rab_anac['operador'].cast(StringType()).alias('ds_operador'),
                        df_rab_anac['ufOperador'].cast(StringType()).alias('cd_estado_operador'),
                        df_rab_anac['cpfCnpjOperador'].cast(StringType()).alias('no_cpf_cnpj_operador'),
                        df_rab_anac['matricula'].cast(StringType()).alias('no_matricula'),
                        df_rab_anac['num_serie'].cast(StringType()).alias('no_serie'),
                        df_rab_anac['categoria'].cast(StringType()).alias('ds_categoria'),
                        df_rab_anac['tipoCert'].cast(StringType()).alias('tp_certificado'),
                        df_rab_anac['modelo'].cast(StringType()).alias('ds_modelo'),
                        df_rab_anac['nomeFabricante'].cast(StringType()).alias('nm_fabricante')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def cnes(df_cnes):
        return (df_cnes
                .select(df_cnes['cpf'].cast(StringType()).alias('no_cpf'),
                        df_cnes['telefone'].cast(StringType()).alias('no_telefone'),
                        df_cnes['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_cnes['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_cnes['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_cnes['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_cnes['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_cnes['cnpjProprio'].cast(StringType()).alias('no_cnpj'),
                        df_cnes['razaoSocial'].cast(StringType()).alias('nm_razao_social'),
                        df_cnes['cnes'].cast(StringType()).alias('no_cnes'),
                        df_cnes['cnpjMantenedora'].cast(StringType()).alias('no_cnpj_mantenedora'),
                        df_cnes['nomeFantasia'].cast(StringType()).alias('nm_fantasia')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def confea(df_confea):
        return (df_confea
                .select(df_confea['cpf'].cast(StringType()).alias('no_cpf'),
                        df_confea['telefone'].cast(StringType()).alias('no_telefone'),
                        df_confea['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_confea['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_confea['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_confea['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_confea['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_confea['nome'].cast(StringType()).alias('nm_confea'),
                        df_confea['rnp'].cast(StringType()).alias('no_rnp'),
                        df_confea['dataRegistro'].cast(StringType()).alias('dt_registro'),
                        df_confea['creaUf'].cast(StringType()).alias('cd_uf_crea'),
                        df_confea['status'].cast(StringType()).alias('ds_status'),
                        df_confea['vistos'].cast(StringType()).alias('ds_vistos'),
                        df_confea['titulos'].cast(StringType()).alias('ds_titulos'),
                        df_confea['posGraduacao'].cast(StringType()).alias('ds_pos_graduacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def coren(df_coren):
        return (df_coren
                .select(df_coren['cpf'].cast(StringType()).alias('no_cpf'),
                        df_coren['telefone'].cast(StringType()).alias('no_telefone'),
                        df_coren['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_coren['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_coren['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_coren['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_coren['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_coren['nome'].cast(StringType()).alias('nm_coren'),
                        df_coren['quadro'].cast(StringType()).alias('ds_quadro'),
                        df_coren['concessao'].cast(StringType()).alias('ds_concessao'),
                        df_coren['dataInscricao'].cast(StringType()).alias('dt_inscricao'),
                        df_coren['status'].cast(StringType()).alias('ds_status'),
                        df_coren['observacao'].cast(StringType()).alias('ds_observacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def siccau(df_siccau):
        return (df_siccau
                .select(df_siccau['cpf'].cast(StringType()).alias('no_cpf'),
                        df_siccau['telefone'].cast(StringType()).alias('no_telefone'),
                        df_siccau['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_siccau['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_siccau['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_siccau['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_siccau['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_siccau['nomeProfissional'].cast(StringType()).alias('nm_profissional'),
                        df_siccau['registroCau'].cast(StringType()).alias('no_registro_cau'),
                        df_siccau['registroAnterior'].cast(StringType()).alias('no_registro_anterior'),
                        df_siccau['ultimaAnuidadePaga'].cast(StringType()).alias('ds_ultima_anuidade_paga'),
                        df_siccau['dtInicioRegistro'].cast(StringType()).alias('dt_inicio_registro'),
                        df_siccau['status'].cast(StringType()).alias('ds_status')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def crosp(df_crosp):
        return (df_crosp
                .select(df_crosp['cpf'].cast(StringType()).alias('no_cpf'),
                        df_crosp['telefone'].cast(StringType()).alias('no_telefone'),
                        df_crosp['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_crosp['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_crosp['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_crosp['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_crosp['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_crosp['nomeProfissional'].cast(StringType()).alias('nm_profissional'),
                        df_crosp['cro'].cast(StringType()).alias('no_cro'),
                        df_crosp['situacao'].cast(StringType()).alias('ds_situacao'),
                        df_crosp['categoria'].cast(StringType()).alias('ds_categoria'),
                        df_crosp['especialidade'].cast(StringType()).alias('ds_especialidade'),
                        df_crosp['habilitacao'].cast(StringType()).alias('ds_habilitacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def cfmv(df_cfmv):
        return (df_cfmv
                .select(df_cfmv['cpf'].cast(StringType()).alias('no_cpf'),
                        df_cfmv['telefone'].cast(StringType()).alias('no_telefone'),
                        df_cfmv['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_cfmv['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_cfmv['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_cfmv['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_cfmv['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_cfmv['nomeProfissional'].cast(StringType()).alias('nm_profissional'),
                        df_cfmv['tipoInscricao'].cast(StringType()).alias('tp_inscricao'),
                        df_cfmv['situacao'].cast(StringType()).alias('ds_situacao'),
                        df_cfmv['area'].cast(StringType()).alias('ds_area'),
                        df_cfmv['uf'].cast(StringType()).alias('cd_estado')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def cfc(df_cfc):
        return (df_cfc
                .select(df_cfc['cpf'].cast(StringType()).alias('no_cpf'),
                        df_cfc['telefone'].cast(StringType()).alias('no_telefone'),
                        df_cfc['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_cfc['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_cfc['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_cfc['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_cfc['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_cfc['nome'].cast(StringType()).alias('nm_cfc'),
                        df_cfc['numeroRegistro'].cast(StringType()).alias('no_registro'),
                        df_cfc['tipoRegistro'].cast(StringType()).alias('tp_registro'),
                        df_cfc['categoria'].cast(StringType()).alias('ds_categoria'),
                        df_cfc['crc'].cast(StringType()).alias('no_crc'),
                        df_cfc['situacao'].cast(StringType()).alias('ds_situacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def telefone_fraude(df_telefone_fraude):
        return (df_telefone_fraude
                .select(df_telefone_fraude['cpf'].cast(StringType()).alias('no_cpf'),
                        df_telefone_fraude['telefone1'].cast(StringType()).alias('no_telefone'),
                        df_telefone_fraude['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_telefone_fraude['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_telefone_fraude['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_telefone_fraude['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_telefone_fraude['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_telefone_fraude['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_telefone_fraude['ddd'].cast(StringType()).alias('no_ddd'),
                        df_telefone_fraude['telefone9'].cast(StringType()).alias('no_telefone_fraude'),
                        df_telefone_fraude['estado'].cast(StringType()).alias('cd_estado')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def anp(df_anp):
        return (df_anp
                .select(df_anp['cpf'].cast(StringType()).alias('no_cpf'),
                        df_anp['telefone'].cast(StringType()).alias('no_telefone'),
                        df_anp['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_anp['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_anp['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_anp['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_anp['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_anp['cnpj'].cast(StringType()).alias('no_cnpj'),
                        df_anp['razao'].cast(StringType()).alias('nm_razao_social'),
                        df_anp['numAutorizacao'].cast(StringType()).alias('no_autorizacao'),
                        df_anp['codSimp'].cast(StringType()).alias('cd_simp'),
                        df_anp['uf'].cast(StringType()).alias('cd_estado'),
                        df_anp['municipio'].cast(StringType()).alias('nm_cidade'),
                        df_anp['complemento'].cast(StringType()).alias('ds_complemento'),
                        df_anp['bairro'].cast(StringType()).alias('ds_bairro'),
                        df_anp['cep'].cast(StringType()).alias('cd_cep'),
                        df_anp['vinculoDistribuidor'].cast(StringType()).alias('ds_vinculo_distribuidor'),
                        df_anp['dataPublicDouAutorizacao'].cast(StringType()).alias('dt_publicacao_dou_autorizacao'),
                        df_anp['dataVinculoDistribuidor'].cast(StringType()).alias('dt_vinculo_distribuidor'),
                        df_anp['tipo'].cast(StringType()).alias('tp_anp')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ppe_prestador_contas_doador(df_prestacao_contas_doador):
        return (df_prestacao_contas_doador
                .select(df_prestacao_contas_doador['cpf'].cast(StringType()).alias('no_cpf'),
                        df_prestacao_contas_doador['telefone'].cast(StringType()).alias('no_telefone'),
                        df_prestacao_contas_doador['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_prestacao_contas_doador['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_prestacao_contas_doador['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_prestacao_contas_doador['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_prestacao_contas_doador['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_prestacao_contas_doador['cpf_cnpj_doador'].cast(StringType()).alias('no_cpf_cnpj'),
                        df_prestacao_contas_doador['nome_doador'].cast(StringType()).alias('nm_doador'),
                        df_prestacao_contas_doador['valor_doado'].cast(StringType()).alias('vl_doado')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def numero_consulta(df_numero_consulta):
        return (df_numero_consulta
                .select(df_numero_consulta['cpf'].cast(StringType()).alias('no_cpf'),
                        df_numero_consulta['telefone'].cast(StringType()).alias('no_telefone'),
                        df_numero_consulta['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_numero_consulta['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_numero_consulta['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_numero_consulta['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_numero_consulta['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_numero_consulta['numeroConsulta'].cast(StringType()).alias('no_consulta')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )


    def ppe_bens(df_ppe_bens):
        return (df_ppe_bens
                .select(df_ppe_bens['cpf'].cast(StringType()).alias('no_cpf'),
                        df_ppe_bens['telefone'].cast(StringType()).alias('no_telefone'),
                        df_ppe_bens['timestamp'].cast(StringType()).alias('dt_execucao'),
                        df_ppe_bens['identificadorJob'].cast(StringType()).alias('ds_job'),
                        df_ppe_bens['sessionId'].cast(StringType()).alias('cd_sessao'),
                        df_ppe_bens['sistemaOrigem'].cast(StringType()).alias('ds_sistema_origem'),
                        df_ppe_bens['tipoTransacao'].cast(StringType()).alias('tp_transacao'),

                        df_ppe_bens['numeroDocumento'].cast(StringType()).alias('no_documento'),
                        df_ppe_bens['nome'].cast(StringType()).alias('nm_ppe_bens'),
                        df_ppe_bens['tipo_bem'].cast(StringType()).alias('tp_bem'),
                        df_ppe_bens['descricao_bem'].cast(StringType()).alias('ds_bem'),
                        df_ppe_bens['valor'].cast(StringType()).alias('vl_bem'),
                        df_ppe_bens['estado'].cast(StringType()).alias('ds_estado'),
                        df_ppe_bens['data_atualizacao'].cast(StringType()).alias('dt_atualizacao')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )

        # funcao que verifica se existe o diretorio


    def valida_diretorio(diretorio):
        try:
            cmd_hdfs = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net" + diretorio
            print (cmd_hdfs)
            saida = int(subprocess.call(cmd_hdfs, shell=True))
            print (saida)
            if saida == 0:
                return True
            else:
                return False
        except Exception as ex:
            print("except")
            return False


    df3 = None
    dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    fmt = '%Y-%m-%d %H:%M:%S'
    d1 = datetime.strptime(dt_inicio, fmt)
    try:
        _dados_io = ArquivosDadosIO('RED_FICO')
        _dadosTabela_io = TabelasDadosIO('RED_FICO')
        _gerenciador = Gerenciador(_dados_io)
        resultado = _gerenciador.gera_regras()
    except Exception as ex:
        df3 = 'ERRO: {}'.format(str(ex))
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        spark.stop()
        exit(1)
    else:
        df3 = ("succeeded")
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)

spark.stop()
exit(0)




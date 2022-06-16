#######################################
######## -*- coding: utf-8 -*-#########
############ Versão - 1.0 #############
####### Financeira - Clientes #########
######## Alex Fernando dos S Silva ####
#######################################

import sys
import os
import subprocess
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

############################################inicio do codigo

processo = sys.argv[1]

# Variável global que retorna a data atual.
current_date = dt.datetime.now().strftime('%d/%m/%Y')
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

    def cliente(self):
        return DadosIO._spark.read.text(
            '/data/harmonized/Financeira/Clientes/diario/FINA539/{}/FINA5391/'.format(
                str(datetime.today().strftime('%Y%m%d'))))

    def objeto_profissao(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ',') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Lookup/objeto_profissao/')

    def objeto_ocupacao(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ',') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Lookup/objeto_profissao/')

    def gravar_parquet(self, df, nome):
        URL = '/hive/warehouse/harmonized.db/h_financeira_clientes/'
        df.write\
            .option("encoding", "UTF-8")\
            .format("parquet")\
            .option("header","false")\
            .insertInto(nome, overwrite=True)

    def path_arquivos(self):
        URL = '/data/harmonized/Financeira/Clientes/diario/FINA539/{}/FINA5391/'.format(
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
        # self._cliente = _dados_io.cliente()
        self._objeto_profissao = _dados_io.objeto_profissao()
        self._objeto_ocupacao = _dados_io.objeto_ocupacao()

    def gera_regras(self):
        # ----------------------------------------------
        _urlFile = self._dados_io.path_arquivos()
        print "_urlFile {}".format(_urlFile)
        _nameDateFile = self._dados_io.retirarDataNameFile(_urlFile, "baseclientes_")
        print "_nameDateFile {}".format(_nameDateFile)
        _dados = _dados_io.carregarArquivo(_urlFile)
        # ----------------------------------------------
        resultado = cliente_posicional(_dados)

        resultado2 = cliente_transformacao(resultado)

        resultado3 = cliente_join_lookup(resultado2, self._objeto_profissao, self._objeto_ocupacao, _nameDateFile)
        return resultado3


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
    # tabela_log_insert.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header", "true").partitionBy('aa_carga').saveAsTable("analytical.a_estrutural_log_execucao_carga")


# Função que retorna os dados do arquivo de Clientes e realiza o tratamento para quebra de colunas.
def cliente_posicional(df_clientes):
    return (df_clientes
            .withColumn('tp_registro', f.substring(df_clientes['value'], 1, 1))
            .withColumn('tp_operacao', f.substring(df_clientes['value'], 2, 1))
            .withColumn('cd_seq_cli', f.substring(df_clientes['value'], 3, 10))
            .withColumn('tp_pessoa', f.substring(df_clientes['value'], 13, 1))
            .withColumn('no_cpf_cnpj', f.substring(df_clientes['value'], 14, 15))
            .withColumn('nm_cliente', f.substring(df_clientes['value'], 29, 50))
            .withColumn('nm_nome_reduzido', f.substring(df_clientes['value'], 79, 20))
            .withColumn('cd_rg', f.substring(df_clientes['value'], 99, 10))
            .withColumn('nm_orgao_emissor', f.substring(df_clientes['value'], 109, 10))
            .withColumn('cd_sexo', f.substring(df_clientes['value'], 119, 1))
            .withColumn('cd_estado_civil', f.substring(df_clientes['value'], 120, 1))
            .withColumn('dt_nascimento', f.substring(df_clientes['value'], 121, 8))
            .withColumn('cd_profissao', f.substring(df_clientes['value'], 129, 5))
            .withColumn('sg_um_org_emiss', f.substring(df_clientes['value'], 134, 2))
            .withColumn('cd_porte_empresa', f.substring(df_clientes['value'], 136, 1))
            .withColumn('cd_ramo_atividade', f.substring(df_clientes['value'], 137, 10))
            .withColumn('cd_ocupacao_cliente', f.substring(df_clientes['value'], 147, 5))
            .withColumn('cd_local_correspondencia', f.substring(df_clientes['value'], 152, 10))
            .withColumn('ds_endereco_residencial', f.substring(df_clientes['value'], 162, 80))
            .withColumn('nm_bairro_residencial', f.substring(df_clientes['value'], 242, 20))
            .withColumn('nm_cidade_residencial', f.substring(df_clientes['value'], 262, 20))
            .withColumn('cd_cep_residencial', f.substring(df_clientes['value'], 282, 8))
            .withColumn('sg_uf_residencial', f.substring(df_clientes['value'], 290, 2))
            .withColumn('nr_ddd_residencial', f.substring(df_clientes['value'], 292, 4))
            .withColumn('nr_telefone_residencial', f.substring(df_clientes['value'], 296, 10))
            .withColumn('ds_endereco_comercial', f.substring(df_clientes['value'], 306, 80))
            .withColumn('nm_bairro_comercial', f.substring(df_clientes['value'], 386, 20))
            .withColumn('nm_cidade_comercial', f.substring(df_clientes['value'], 406, 20))
            .withColumn('cd_cep_comercial', f.substring(df_clientes['value'], 426, 8))
            .withColumn('sg_uf_comercial', f.substring(df_clientes['value'], 434, 2))
            .withColumn('cd_conta_banco_cliente', f.substring(df_clientes['value'], 436, 15))
            .withColumn('cd_agencia_conta_corrente', f.substring(df_clientes['value'], 451, 15))
            .withColumn('nr_conta_corrente_cliente', f.substring(df_clientes['value'], 466, 20))
            .withColumn('nr_ddd_comercial', f.substring(df_clientes['value'], 486, 4))
            .withColumn('nr_telefone_comercial', f.substring(df_clientes['value'], 490, 10))
            .withColumn('nm_atividade', f.substring(df_clientes['value'], 500, 30))
            .withColumn('vl_renda', f.concat(f.substring(df_clientes['value'], 530, 13), f.lit('.'),
                                             f.substring(df_clientes['value'], 543, 2)))
            .withColumn('nr_ddd_celular', f.substring(df_clientes['value'], 545, 10))
            .withColumn('nr_telefone_celular', f.substring(df_clientes['value'], 555, 10))
            .withColumn('ds_email', f.substring(df_clientes['value'], 565, 60))
            .withColumn('filler', f.substring(df_clientes['value'], 625, 26))
            .drop('value')
            )


# Função que realiza os tratamentos de datatypes e colocar a string em maiúsculo.
def cliente_transformacao(cliente):
    return (cliente
            .select(cliente['tp_registro'].cast(IntegerType()),
                    ut_f.limpa_espaco_branco(f.upper(f.when(f.upper(cliente['tp_operacao']) == 'I', 'Inclusao')
                        .otherwise(f.when(f.upper(cliente['tp_operacao']) == 'A', 'Alteracao')
                        .otherwise(
                        f.when(f.upper(cliente['tp_operacao']) == 'E', 'Exclusao').otherwise(
                            cliente['tp_operacao']))))).alias('tp_operacao'),
                    cliente['cd_seq_cli'].cast('bigint'),
                    ut_f.limpa_espaco_branco(f.upper(f.when(f.upper(cliente['tp_pessoa']) == 'F', 'PESSOA FÍSICA')
                        .otherwise(f.when(f.upper(cliente['tp_pessoa']) == 'J', 'PESSOA JURÍDICA').otherwise(
                        cliente['tp_pessoa'])))).alias('tp_pessoa'),
                    ut_f.limpa_espaco_branco(cliente['no_cpf_cnpj']).alias('no_cpf_cnpj'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_cliente'])).alias('nm_cliente'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_nome_reduzido'])).alias('nm_nome_reduzido'),
                    cliente['cd_rg'].cast('bigint'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_orgao_emissor'])).alias('nm_orgao_emissor'),
                    ut_f.limpa_espaco_branco(f.upper(f.when(f.upper(cliente['cd_sexo']) == 'M', 'MASCULINO')
                        .otherwise(f.when(f.upper(cliente['cd_sexo']) == 'F', 'FEMININO').otherwise(
                        cliente['cd_sexo'])))).alias('ds_sexo'),
                    ut_f.limpa_espaco_branco(f.upper(f.when(f.upper(cliente['cd_estado_civil']) == 'S', 'SOLTEIRO')
                        .otherwise(f.when(f.upper(cliente['cd_estado_civil']) == 'C', 'CASADO')
                        .otherwise(f.when(f.upper(cliente['cd_estado_civil']) == 'D', 'DIVORCIADO')
                        .otherwise(
                        f.when(f.upper(cliente['cd_estado_civil']) == 'Q', 'DESQUITADO')
                            .otherwise(f.when(f.upper(cliente['cd_estado_civil']) == 'O', 'OUTROS').otherwise(
                            cliente['cd_estado_civil']))))))).alias('ds_estado_civil'),
                    cliente['dt_nascimento'].cast(IntegerType()),
                    ut_f.limpa_espaco_branco(cliente['cd_profissao']).alias('cd_profissao'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['sg_um_org_emiss'])).alias('sg_um_org_emiss'),
                    ut_f.limpa_espaco_branco(f.upper(f.when(f.upper(cliente['cd_porte_empresa']) == 'P', 'PEQUENA')
                        .otherwise(f.when(f.upper(cliente['cd_porte_empresa']) == 'M', 'MÉDIA')
                        .otherwise(f.when(f.upper(cliente['cd_porte_empresa']) == 'I', 'INDÚSTRIA')
                        .otherwise(
                        f.when(f.upper(cliente['cd_porte_empresa']) == 'N', 'OUTROS').otherwise(
                            cliente['cd_porte_empresa'])))))).alias('ds_porte_empresa'),
                    ut_f.limpa_espaco_branco(cliente['cd_ramo_atividade']).alias('cd_ramo_atividade'),
                    ut_f.limpa_espaco_branco(cliente['cd_ocupacao_cliente']).alias('cd_ocupacao_cliente'),
                    ut_f.limpa_espaco_branco(
                        f.upper(f.when(f.trim(cliente['cd_local_correspondencia']) == '1', 'ENDEREÇO RESIDENCIAL')
                            .otherwise(
                            f.when(f.trim(cliente['cd_local_correspondencia']) == '2', 'ENDEREÇO COMERCIAL').otherwise(
                                cliente['cd_local_correspondencia'])))).alias('ds_local_correspondencia'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['ds_endereco_residencial'])).alias(
                        'ds_endereco_residencial'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_bairro_residencial'])).alias('nm_bairro_residencial'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_cidade_residencial'])).alias('nm_cidade_residencial'),
                    ut_f.limpa_espaco_branco(cliente['cd_cep_residencial']).alias('cd_cep_residencial'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['sg_uf_residencial'])).alias('sg_uf_residencial'),
                    cliente['nr_ddd_residencial'].cast(IntegerType()),
                    cliente['nr_telefone_residencial'].cast('bigint'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['ds_endereco_comercial'])).alias('ds_endereco_comercial'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_bairro_comercial'])).alias('nm_bairro_comercial'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_cidade_comercial'])).alias('nm_cidade_comercial'),
                    cliente['cd_cep_comercial'],
                    ut_f.limpa_espaco_branco(f.upper(cliente['sg_uf_comercial'])).alias('sg_uf_comercial'),
                    cliente['cd_conta_banco_cliente'].cast(IntegerType()),
                    cliente['cd_agencia_conta_corrente'].cast(IntegerType()),
                    f.trim(cliente['nr_conta_corrente_cliente']).cast('bigint').alias('nr_conta_corrente_cliente'),
                    cliente['nr_ddd_comercial'].cast(IntegerType()),
                    cliente['nr_telefone_comercial'].cast('bigint'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['nm_atividade'])).alias('nm_atividade'),
                    cliente['vl_renda'].cast(DecimalType(13, 2)),
                    f.trim(cliente['nr_ddd_celular']).cast(IntegerType()).alias('nr_ddd_celular'),
                    cliente['nr_telefone_celular'].cast('bigint'),
                    ut_f.limpa_espaco_branco(f.upper(cliente['ds_email'])).alias('ds_email'),
                    ut_f.limpa_espaco_branco(cliente['filler']).alias('filler'))
            )


# Função que realiza o join com a tabela de lookup de ocupação e profissão.
def cliente_join_lookup(cliente_transformacao, ocupacao_cliente, profissao_cliente, _nameDateFile):
    return (cliente_transformacao
            .join(ocupacao_cliente,
                  cliente_transformacao['cd_ocupacao_cliente'] == ocupacao_cliente['CD_PROFISSAO'], 'left')
            .join(profissao_cliente, cliente_transformacao['cd_profissao'] == profissao_cliente['CD_PROFISSAO'],
                  'left')
            .select(cliente_transformacao['tp_registro'],
                    cliente_transformacao['tp_operacao'],
                    cliente_transformacao['cd_seq_cli'],
                    cliente_transformacao['tp_pessoa'],
                    cliente_transformacao['no_cpf_cnpj'],
                    cliente_transformacao['nm_cliente'],
                    cliente_transformacao['nm_nome_reduzido'],
                    cliente_transformacao['cd_rg'],
                    cliente_transformacao['nm_orgao_emissor'],
                    cliente_transformacao['ds_sexo'],
                    cliente_transformacao['ds_estado_civil'],
                    cliente_transformacao['dt_nascimento'],
                    cliente_transformacao['cd_profissao'],
                    f.upper(profissao_cliente['DS_PROFISSAO']).alias('ds_profissao'),
                    cliente_transformacao['sg_um_org_emiss'],
                    cliente_transformacao['ds_porte_empresa'],
                    cliente_transformacao['cd_ramo_atividade'],
                    cliente_transformacao['cd_ocupacao_cliente'],
                    f.upper(ocupacao_cliente['DS_PROFISSAO']).alias('ds_ocupacao_cliente'),
                    cliente_transformacao['ds_local_correspondencia'],
                    cliente_transformacao['ds_endereco_residencial'],
                    cliente_transformacao['nm_bairro_residencial'],
                    cliente_transformacao['nm_cidade_residencial'],
                    cliente_transformacao['cd_cep_residencial'],
                    cliente_transformacao['sg_uf_residencial'],
                    cliente_transformacao['nr_ddd_residencial'],
                    cliente_transformacao['nr_telefone_residencial'],
                    cliente_transformacao['ds_endereco_comercial'],
                    cliente_transformacao['nm_bairro_comercial'],
                    cliente_transformacao['nm_cidade_comercial'],
                    cliente_transformacao['cd_cep_comercial'],
                    cliente_transformacao['sg_uf_comercial'],
                    cliente_transformacao['cd_conta_banco_cliente'],
                    cliente_transformacao['cd_agencia_conta_corrente'],
                    cliente_transformacao['nr_conta_corrente_cliente'],
                    cliente_transformacao['nr_ddd_comercial'],
                    cliente_transformacao['nr_telefone_comercial'],
                    cliente_transformacao['nm_atividade'],
                    cliente_transformacao['vl_renda'],
                    cliente_transformacao['nr_ddd_celular'],
                    cliente_transformacao['nr_telefone_celular'],
                    cliente_transformacao['ds_email'],
                    cliente_transformacao['filler']
                    )
            .withColumn('dt_carga', f.lit(str(_nameDateFile)))
            # .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
            )


df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)
try:
    _dados_io = DadosIO('RED_CLIENTES')

    _gerenciador = Gerenciador(_dados_io)
    df_bens_e_garantias = _gerenciador.gera_regras()
    tabela_hive = 'harmonized.h_financeira_clientes'
    _dados_io.gravar_parquet(df_bens_e_garantias, tabela_hive)
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
    exit(0)

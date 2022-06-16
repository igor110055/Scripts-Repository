######################################
######## -*- coding: utf-8 -*-########
########### ARQUIVO PROPOSTAS ########
########### TRILHA 04-  ##############
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
from py4j.java_gateway import java_import
from sys import argv
from itertools import chain
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
##################################inicio de codigo
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

    def proposta_clientes(self):
        return DadosIO._spark.read.text(
            '/data/harmonized/Financeira/ADP/diario/ADPB05N/{}/ADPB05N3/'.format(
                str(datetime.today().strftime('%Y%m%d'))))
        #  '/data/harmonized/Financeira/ADP/diario/ADPB05N/20190619/ADPB05N3/')

    def df_objeto_financiado(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ';') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Financeira/Lookup/Objeto_financiado/')

    def lookup_ocupacao(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ';') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Financeira/Lookup/objeto_ocupacao/')

    def df_produtos(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ';') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Financeira/Lookup/Produtos/')

    def lookup_profissao(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'true') \
            .option('delimiter', ';') \
            .option('inferSchema', 'true') \
            .load('/data/raw/Financeira/Lookup/objeto_profissao/')

    def gravar_parquet(self, df, nome):
        URL = '/hive/warehouse/harmonized.db/h_financeira_propostas_cliente/'
        df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header",
                                                                                        "false").partitionBy(
            'dt_carga').saveAsTable(nome, path='/data/harmonized/Financeira/ADP/h_financeira_propostas_cliente')

    def path_arquivos(self):
        URL ='/data/harmonized/Financeira/ADP/diario/ADPB05N/{}/ADPB05N3/'.format(str(datetime.today().strftime('%Y%m%d')))
        return self.listFilesHDFS(URL)

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
        self._lookup_ocupacao = _dados_io.lookup_ocupacao()
        self._lookup_profissao = _dados_io.lookup_profissao()


    def normaliza_tabela(self, tabela_original):
        tabela_normalizada = lookup_ocupacao(tabela_original, self._lookup_ocupacao)
        tabela_normalizada = lookup_ocupacao_conjuge(tabela_normalizada, self._lookup_ocupacao)
        tabela_normalizada = lookup_profissao(tabela_normalizada, self._lookup_profissao)
        tabela_normalizada = lookup_profissao_conjuge(tabela_normalizada, self._lookup_profissao)
        return tabela_normalizada

    def gera_regras(self):
        #----------------------------------------------
        _urlFile = self._dados_io.path_arquivos()
        print "_urlFile {}".format(_urlFile)
        _nameDateFile = self._dados_io.retirarDataNameFile(_urlFile, "basepropostas_")
        print "_nameDateFile {}".format(_nameDateFile)
        _dados = _dados_io.carregarArquivo(_urlFile)
        #----------------------------------------------

        result = colunas_proposta_clientes(_dados, _nameDateFile)
        result2 = self.normaliza_tabela(result)
        return result2


def colunas_proposta_clientes(df, _nameDateFile):
    # Tratamentos de lookup
    lookup_sexo = {'M': 'MASCULINO', 'F': 'FEMININO'}
    mapping_sexo = f.create_map([f.lit(x) for x in chain(*lookup_sexo.items())])

    lookup_cd_pessoa = {1: 'TITULAR', 2: 'CONJUGE TITULAR', 3: 'AVALISTA', 5: 'AVALISTA', 7: 'FIEL DEPOSIT'}
    mapping_cd_pessoa = f.create_map([f.lit(x) for x in chain(*lookup_cd_pessoa.items())])

    lookup_cd_estado_civil = {1: 'SOLTEIRO', 2: 'CASADO', 3: 'SEPARADO', 4: 'VIUVO', 5: 'DIVORCIADO', 6: 'OUTROS'}
    mapping_cd_estado_civil = f.create_map([f.lit(x) for x in chain(*lookup_cd_estado_civil.items())])

    lookup_cd_local_correspondencia = {1: 'ENDERECO RESIDENCIAL', 2: 'ENDERECO COMERCIAL'}
    mapping_cd_local_correspondencia = f.create_map(
        [f.lit(x) for x in chain(*lookup_cd_local_correspondencia.items())])

    lookup_cd_tipo_pessoa = {'F': 'FÍSICA', 'J': 'JURÍDICA'}
    mapping_cd_tipo_pessoa = f.create_map([f.lit(x) for x in chain(*lookup_cd_tipo_pessoa.items())])

    result = (df
              .withColumn('tp_registro', f.substring(df['value'], 1, 1).cast(t.IntegerType()))
              .withColumn('cd_proposta', f.substring(df['value'], 2, 10).cast('bigint'))
              .withColumn('cd_pessoa', f.substring(df['value'], 12, 1).cast(t.IntegerType()))
              .withColumn('ds_pessoa', ut_f.limpa_espaco_branco(mapping_cd_pessoa[f.substring(df['value'], 12, 1)]))
              .withColumn('no_cpf_cnpj', f.lpad(f.substring(df['value'], 13, 15).cast('bigint'), 15, "0"))
              .withColumn('no_cliente', f.substring(df['value'], 28, 10).cast('bigint'))
              .withColumn('nm_pessoa', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 38, 40))))
              .withColumn('dt_nascimento',
                          f.from_unixtime(f.unix_timestamp(f.substring(df['value'], 78, 8), 'ddMMyyy'),
                                          'yyyMMdd').cast(t.IntegerType()))
              .withColumn('cd_estado', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 86, 2))))
              .withColumn('cd_nacionalidade', f.substring(df['value'], 88, 1).cast(t.IntegerType()))
              .withColumn('ds_sexo', ut_f.limpa_espaco_branco(mapping_sexo[f.upper(f.substring(df['value'], 89, 1))]))
              .withColumn('cd_estado_civil', f.substring(df['value'], 90, 1).cast(t.IntegerType()))
              .withColumn('ds_estado_civil', ut_f.limpa_espaco_branco(mapping_cd_estado_civil[f.upper(f.substring(df['value'], 90, 1))]))
              .withColumn('cd_grau_instrucao', f.substring(df['value'], 91, 1).cast(t.IntegerType()))
              .withColumn('no_dependentes', f.substring(df['value'], 92, 2).cast(t.IntegerType()))
              .withColumn('cd_cep_residencia', f.lpad(f.substring(df['value'], 94, 8).cast('bigint'), 8, "0"))
              .withColumn('qt_tempo_residencia', f.substring(df['value'], 102, 8).cast(t.IntegerType()))
              .withColumn('dt_residencia',
                          f.from_unixtime(f.unix_timestamp(f.substring(df['value'], 110, 8), 'ddMMyyy'),
                                          'yyyMMdd').cast(t.IntegerType()))
              .withColumn('cd_tipo_residencia', f.substring(df['value'], 118, 1).cast(t.IntegerType()))
              .withColumn('ic_telefone_residencia', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 119, 1))))
              .withColumn('no_telefone_residencia', f.substring(df['value'], 120, 10).cast('bigint'))
              .withColumn('no_ddd_residencia', f.substring(df['value'], 130, 3).cast(t.IntegerType()))
              .withColumn('cd_tipo_telefone', f.substring(df['value'], 133, 1).cast(t.IntegerType()))
              .withColumn('qt_tempo_empresa', f.substring(df['value'], 134, 4).cast(t.IntegerType()))
              .withColumn('dt_admissao',
                          f.from_unixtime(f.unix_timestamp(f.substring(df['value'], 138, 8), 'ddMMyyy'),
                                          'yyyMMdd').cast(t.IntegerType()))
              .withColumn('cd_cep_comercial', f.lpad(f.substring(df['value'], 146, 8).cast('bigint'), 8, "0"))
              .withColumn('ic_telefone_comercial', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 154, 1))))
              .withColumn('no_telefone_comercial', f.substring(df['value'], 155, 10).cast('bigint'))
              .withColumn('ic_telefone_com_res', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 165, 1))))
              .withColumn('ic_telefone_com_cel', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 166, 1))))
              .withColumn('ic_telefone_res_cel', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 167, 1))))
              .withColumn('cd_local_correspondencia', f.substring(df['value'], 168, 1).cast(t.IntegerType()))
              .withColumn('ds_local_correspondencia',
                          ut_f.limpa_espaco_branco(mapping_cd_local_correspondencia[f.upper(f.substring(df['value'], 168, 1))]))
              .withColumn('cd_ocupacao', f.substring(df['value'], 199, 4).cast(t.IntegerType()))
              .withColumn('ds_tipo_pessoa', ut_f.limpa_espaco_branco(mapping_cd_tipo_pessoa[f.upper(f.substring(df['value'], 203, 1))]))
              .withColumn('cd_profissao', f.substring(df['value'], 204, 5).cast(t.IntegerType()))
              .withColumn('ds_profissao', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 169, 30))))
              .withColumn('cd_natureza', f.substring(df['value'], 209, 1).cast(t.IntegerType()))
              .withColumn('pc_compr_renda', f.substring(df['value'], 210, 2).cast(t.IntegerType()))
              .withColumn('tipo_renda', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 212, 1))))
              .withColumn('vl_renda',
                          f.concat(f.substring(df['value'], 213, 15), f.lit('.'),
                                   f.substring(df['value'], 228, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('vl_outra_renda',
                          f.concat(f.substring(df['value'], 230, 15), f.lit('.'),
                                   f.substring(df['value'], 245, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('qt_tempo_empresa_anterior', f.substring(df['value'], 247, 4).cast(t.IntegerType()))
              .withColumn('dt_admissao_anterior',
                          f.from_unixtime(f.unix_timestamp(f.substring(df['value'], 251, 8), 'ddMMyyy'),
                                          'yyyMMdd').cast(t.IntegerType()))
              .withColumn('dt_nascimento_conjuge',
                          f.from_unixtime(f.unix_timestamp(f.substring(df['value'], 259, 8), 'ddMMyyy'),
                                          'yyyMMdd').cast(t.IntegerType()))
              .withColumn('no_cpf_cnpj_conjuge', f.lpad(f.substring(df['value'], 267, 15).cast('bigint'), 15, "0"))
              .withColumn('cd_ocupacao_conjuge', f.substring(df['value'], 312, 4).cast(t.IntegerType()))
              .withColumn('cd_profissao_conjuge', f.substring(df['value'], 316, 5).cast(t.IntegerType()))
              .withColumn('ds_profissao_conjuge', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 282, 30))))
              .withColumn('pc_compr_renda_conjuge', f.substring(df['value'], 321, 2).cast(t.IntegerType()))
              .withColumn('tp_renda_conjuge', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 323, 1))))
              .withColumn('vl_renda_conjuge',
                          f.concat(f.substring(df['value'], 324, 15), f.lit('.'),
                                   f.substring(df['value'], 339, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('pc_compr_renda_familia', f.substring(df['value'], 341, 2).cast(t.IntegerType()))
              .withColumn('vl_renda_familia',
                          f.concat(f.substring(df['value'], 343, 15), f.lit('.'),
                                   f.substring(df['value'], 358, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('no_telefone_com_conjuge', f.substring(df['value'], 360, 10).cast('bigint'))
              .withColumn('cd_serasa_titular', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 370, 1))))
              .withColumn('cd_serasa_conjuge', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 371, 1))))
              .withColumn('ic_golpe_estel', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 372, 1))))
              .withColumn('ic_funcionario', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 373, 1))))
              .withColumn('cd_spc_titular', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 374, 1))))
              .withColumn('cd_spc_conjuge', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 375, 1))))
              .withColumn('cd_exp_abn', f.substring(df['value'], 376, 1).cast(t.IntegerType()))
              .withColumn('cd_lt', f.substring(df['value'], 377, 1).cast(t.IntegerType()))
              .withColumn('cd_cr', f.substring(df['value'], 378, 1).cast(t.IntegerType()))
              .withColumn('cd_lr', f.substring(df['value'], 379, 1).cast(t.IntegerType()))
              .withColumn('cd_banco', f.substring(df['value'], 380, 3).cast(t.IntegerType()))
              .withColumn('no_telefone_referencia1', f.substring(df['value'], 383, 10).cast('bigint'))
              .withColumn('no_telefone_referencia2', f.substring(df['value'], 393, 10).cast('bigint'))
              .withColumn('qt_imoveis', f.substring(df['value'], 403, 3).cast(t.IntegerType()))
              .withColumn('vl_veiculo_proprio',
                          f.concat(f.substring(df['value'], 406, 15), f.lit('.'),
                                   f.substring(df['value'], 421, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('cd_marca_veiculo', f.substring(df['value'], 423, 3).cast(t.IntegerType()))
              .withColumn('cd_modelo_veiculo', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 426, 4))))
              .withColumn('aa_veiculo', f.substring(df['value'], 430, 4).cast(t.IntegerType()))
              .withColumn('cd_cartao', f.substring(df['value'], 434, 1).cast(t.IntegerType()))
              .withColumn('ic_visa', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 435, 1))))
              .withColumn('ic_mastercard', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 436, 1))))
              .withColumn('ic_diners', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 437, 1))))
              .withColumn('ic_sollo', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 438, 1))))
              .withColumn('ic_credcard', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 439, 1))))
              .withColumn('ic_amex', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 440, 1))))
              .withColumn('ic_outro', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 441, 1))))
              .withColumn('ic_cheque_especial', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 442, 1))))
              .withColumn('grau_parentesco', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 443, 3))))
              .withColumn('vl_renda_presumida',
                          f.concat(f.substring(df['value'], 446, 15), f.lit('.'),
                                   f.substring(df['value'], 461, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('idade', f.substring(df['value'], 463, 3).cast(t.IntegerType()))
              .withColumn('cd_registro', f.substring(df['value'], 466, 1).cast(t.IntegerType()))
              .withColumn('vl_renda_conjunta',
                          f.concat(f.substring(df['value'], 467, 15), f.lit('.'),
                                   f.substring(df['value'], 482, 2)).cast(t.DecimalType(17, 2)))
              .withColumn('ic_cep_residencia', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 484, 1))))
              .withColumn('ic_cep_comercial', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 485, 1))))
              .withColumn('cd_grau_relacionamento', f.substring(df['value'], 486, 2).cast(t.IntegerType()))
              .withColumn('e_mail', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 488, 60))))
              .withColumn('cd_crb', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 548, 1))))
              .withColumn('sit_cpfcnpj_rec', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 549, 1))))
              .withColumn('sit_emp_rec', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 550, 1))))
              .withColumn('cd_rating', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 551, 2))))
              .withColumn('ls_real', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 553, 2))))
              .withColumn('ls_bandepe', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 555, 2))))
              .withColumn('ls_sudameris', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 557, 2))))
              .withColumn('cd_rating_emp', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 559, 2))))
              .withColumn('ls_real_emp', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 561, 2))))
              .withColumn('ls_bandepe_emp', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 563, 2))))
              .withColumn('ls_sudameris_emp', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 565, 2))))
              .withColumn('cd_agencia', f.substring(df['value'], 567, 4).cast(t.IntegerType()))
              .withColumn('no_conta_corrente', f.substring(df['value'], 571, 13).cast('bigint'))
              .withColumn('no_ddd_telefone', f.substring(df['value'], 584, 4).cast(t.IntegerType()))
              .withColumn('no_telefone', f.substring(df['value'], 588, 10).cast('bigint'))
              .withColumn('nm_referencia1', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 598, 50))))
              .withColumn('no_ddd_telefone_referencia1', f.substring(df['value'], 648, 4).cast(t.IntegerType()))
              .withColumn('ds_endereco_residencial', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 652, 50))))
              .withColumn('nm_referencia2', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 702, 50))))
              .withColumn('no_ddd_telefone_referencia2', f.substring(df['value'], 752, 4).cast(t.IntegerType()))
              .withColumn('ds_endereco_residencia2', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 756, 50))))
              .withColumn('ls_aymore', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 806, 2))))
              .withColumn('cd_classificacao_cliente', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 808, 1))))
              .withColumn('ic_alerta_revisional', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 809, 1))))
              .withColumn('ct_inconsistencia_occurs', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 810, 1))))
              .withColumn('cd_score', f.substring(df['value'], 817, 7).cast(t.IntegerType()))
              .withColumn('cd_ab01', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 824, 1))))
              .withColumn('cd_ab02', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 825, 1))))
              .withColumn('cd_ab03', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 826, 1))))
              .withColumn('cd_ab04', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 827, 1))))
              .withColumn('cd_ab05', f.substring(df['value'], 828, 3).cast(t.IntegerType()))
              .withColumn('cd_ab06', f.substring(df['value'], 831, 3).cast(t.IntegerType()))
              .withColumn('cd_ab07', f.substring(df['value'], 834, 3).cast(t.IntegerType()))
              .withColumn('cd_ab08', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 837, 1))))
              .withColumn('cd_ab09', f.substring(df['value'], 838, 3).cast(t.IntegerType()))
              .withColumn('cd_ab10', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 841, 1))))
              .withColumn('cd_ab11', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 842, 1))))
              .withColumn('cd_ab12', f.substring(df['value'], 843, 8).cast(t.IntegerType()))
              .withColumn('cd_ab13', f.substring(df['value'], 851, 3).cast(t.IntegerType()))
              .withColumn('cd_ab14', f.substring(df['value'], 854, 3).cast(t.IntegerType()))
              .withColumn('cd_ab15', f.substring(df['value'], 857, 3).cast(t.IntegerType()))
              .withColumn('cd_ae01', f.substring(df['value'], 860, 3).cast(t.IntegerType()))
              .withColumn('cd_ae02', f.substring(df['value'], 863, 3).cast(t.IntegerType()))
              .withColumn('cd_ae03', f.substring(df['value'], 866, 3).cast(t.IntegerType()))
              .withColumn('cd_ae04', f.substring(df['value'], 869, 3).cast(t.IntegerType()))
              .withColumn('cd_ae05', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 872, 1))))
              .withColumn('cd_ae06', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 873, 1))))
              .withColumn('cd_ae07', f.substring(df['value'], 874, 3).cast(t.IntegerType()))
              .withColumn('cd_ae08', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 877, 1))))
              .withColumn('cd_ae09', f.substring(df['value'], 878, 3).cast(t.IntegerType()))
              .withColumn('cd_ae10', f.substring(df['value'], 881, 3).cast(t.IntegerType()))
              .withColumn('cd_ao01', f.substring(df['value'], 884, 8).cast(t.IntegerType()))
              .withColumn('cd_ao02', f.substring(df['value'], 892, 8).cast(t.IntegerType()))
              .withColumn('cd_ap01', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 900, 1))))
              .withColumn('cd_at01', f.substring(df['value'], 901, 3).cast(t.IntegerType()))
              .withColumn('cd_at02', f.substring(df['value'], 904, 3).cast(t.IntegerType()))
              .withColumn('cd_at03', f.substring(df['value'], 907, 3).cast(t.IntegerType()))
              .withColumn('cd_at04', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 910, 1))))
              .withColumn('cd_at05', f.substring(df['value'], 911, 3).cast(t.IntegerType()))
              .withColumn('cd_at06', f.substring(df['value'], 914, 3).cast(t.IntegerType()))
              .withColumn('cd_at07', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 917, 1))))
              .withColumn('ds_sexo_conjuge', ut_f.limpa_espaco_branco(mapping_sexo[f.upper(f.substring(df['value'], 918, 1))]))
              .withColumn('pes_polit_exp', f.substring(df['value'], 919, 1).cast(t.IntegerType()))
              .withColumn('vl_patrimonio', f.concat(f.substring(df['value'], 920, 11), f.lit('.'),
                                                    f.substring(df['value'], 931, 2)).cast(t.DecimalType(13, 2)))
              .withColumn('qt_penfin_resolv_6m', f.substring(df['value'], 933, 4).cast(t.IntegerType()))
              .withColumn('qt_penfin_resolv_12m', f.substring(df['value'], 937, 4).cast(t.IntegerType()))
              .withColumn('qt_ocorrencia_ativo_serasa', f.substring(df['value'], 941, 4).cast(t.IntegerType()))
              .withColumn('tmp_ocorrencia_ativo_negativo_m_rec',
                          f.substring(df['value'], 945, 5).cast(t.IntegerType()))
              .withColumn('tmp_ocorrencia_resolv_neg_m_rec', f.substring(df['value'], 950, 5).cast(t.IntegerType()))
              .withColumn('qt_ocorrencia_acm_1d_12m', f.substring(df['value'], 955, 4).cast(t.IntegerType()))
              .withColumn('qt_ocorrencia_acm_90d_12m', f.substring(df['value'], 959, 4).cast(t.IntegerType()))
              .withColumn('vl_maximo_ocorrencia_neg_12m',
                          f.concat(f.substring(df['value'], 963, 13), f.lit('.'),
                                   f.substring(df['value'], 976, 2)).cast(t.DecimalType(15, 2)))
              .withColumn('tmp_ocorrencia_neg_rec_12m', f.substring(df['value'], 978, 4).cast(t.IntegerType()))
              .withColumn('vl_total_ocorrencia_neg_12m',
                          f.concat(f.substring(df['value'], 982, 13), f.lit('.'),
                                   f.substring(df['value'], 995, 2)).cast(t.DecimalType(15, 2)))
              .withColumn('tmp_medio_restritivo_resolv_sccf_12m',
                          f.substring(df['value'], 997, 5).cast(t.IntegerType()))
              .withColumn('qt_ccf_ativo_12m', f.substring(df['value'], 1002, 5).cast(t.IntegerType()))
              .withColumn('qt_ccf_resolv_12m', f.substring(df['value'], 1007, 5).cast(t.IntegerType()))
              .withColumn('qt_ccf_ativo', f.substring(df['value'], 1012, 5).cast(t.IntegerType()))
              .withColumn('qt_acoes_resolv_12m', f.substring(df['value'], 1017, 5).cast(t.IntegerType()))
              .withColumn('qt_refin_ativo', f.substring(df['value'], 1022, 4).cast(t.IntegerType()))
              .withColumn('qt_refin_ativo_12m', f.substring(df['value'], 1026, 4).cast(t.IntegerType()))
              .withColumn('qt_refin_resolv_6m', f.substring(df['value'], 1030, 4).cast(t.IntegerType()))
              .withColumn('qt_refin_resolv_12m', f.substring(df['value'], 1034, 4).cast(t.IntegerType()))
              .withColumn('qt_pefin_ativo', f.substring(df['value'], 1038, 4).cast(t.IntegerType()))
              .withColumn('qt_pefin_ativo_12m', f.substring(df['value'], 1042, 4).cast(t.IntegerType()))
              .withColumn('qt_protesto_ativo', f.substring(df['value'], 1046, 5).cast(t.IntegerType()))
              .withColumn('qt_protesto_res_12m', f.substring(df['value'], 1051, 4).cast(t.IntegerType()))
              .withColumn('qt_restritivo_ativo_res_12m', f.substring(df['value'], 1055, 5).cast(t.IntegerType()))
              .withColumn('qt_acoes_ativo', f.substring(df['value'], 1060, 5).cast(t.IntegerType()))
              .withColumn('ic_consulta_cadus', f.substring(df['value'], 1065, 1).cast(t.IntegerType()))
              .withColumn('nt_bhv', f.substring(df['value'], 1066, 5).cast(t.IntegerType()))
              .withColumn('nm_modelo_bhv', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1071, 5))))
              .withColumn('fx_pd_bhv', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1076, 5))))
              .withColumn('st_consulta_bhv', f.substring(df['value'], 1081, 1).cast(t.IntegerType()))
              .withColumn('cd_score_seg_th', f.substring(df['value'], 1082, 11).cast(t.IntegerType()))
              .withColumn('vl_lim_pre_aprov_th', f.substring(df['value'], 1093, 9).cast(t.IntegerType()))
              .withColumn('st_consulta_th', f.substring(df['value'], 1102, 1).cast(t.IntegerType()))
              .withColumn('vl_maximo_renda_presum', f.substring(df['value'], 1103, 15).cast('bigint'))
              .withColumn('cd_grau_severidade', f.substring(df['value'], 1118, 3).cast(t.IntegerType()))
              .withColumn('nr_6e7digito_cpf', f.substring(df['value'], 1121, 2).cast(t.IntegerType()))
              .withColumn('nr_9digito_cpf', f.substring(df['value'], 1123, 1).cast(t.IntegerType()))
              .withColumn('dd_sem_inc_prop', f.substring(df['value'], 1124, 1).cast(t.IntegerType()))
              .withColumn('hr_inc_prop', f.substring(df['value'], 1125, 4).cast(t.IntegerType()))
              .withColumn('cd_ativ_econ_ir', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1129, 7))))
              .withColumn('ic_possui_veiculo', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1136, 1))))
              .withColumn('qt_veiculo_atuais', f.substring(df['value'], 1137, 3).cast(t.IntegerType()))
              .withColumn('qt_veiculo_hist', f.substring(df['value'], 1140, 3).cast(t.IntegerType()))
              .withColumn('tp_ult_alienacao', f.substring(df['value'], 1143, 3).cast(t.IntegerType()))
              .withColumn('tp_fora_merc', f.substring(df['value'], 1146, 3).cast(t.IntegerType()))
              .withColumn('sinal_01', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1149, 1))))
              .withColumn('vl_gar_med_veiculo_total', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1150, 3))))
              .withColumn('sinal_02', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1153, 1))))
              .withColumn('vl_gar_med_veiculo_fin', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1154, 3))))
              .withColumn('sinal_03', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1157, 1))))
              .withColumn('vl_gar_med_veiculo_qui', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1158, 3))))
              .withColumn('sinal_04', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1161, 1))))
              .withColumn('vl_gar_med_veiculo_hst', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1162, 3))))
              .withColumn('vl_med_veiculo_totais', f.substring(df['value'], 1165, 6).cast(t.IntegerType()))
              .withColumn('qt_veiculo_atu_fin', f.substring(df['value'], 1171, 3).cast(t.IntegerType()))
              .withColumn('vl_med_veiculo_fin', f.substring(df['value'], 1174, 6).cast(t.IntegerType()))
              .withColumn('vl_total_veiculo_atu', f.substring(df['value'], 1180, 6).cast(t.IntegerType()))
              .withColumn('qt_total_veiculo_fin', f.substring(df['value'], 1186, 3).cast(t.IntegerType()))
              .withColumn('vl_med_veiculo_fin_hst', f.substring(df['value'], 1189, 6).cast(t.IntegerType()))
              .withColumn('qt_tot_veiculo_qui', f.substring(df['value'], 1195, 3).cast(t.IntegerType()))
              .withColumn('vl_med_veiculo_qui_hst', f.substring(df['value'], 1198, 6).cast(t.IntegerType()))
              .withColumn('st_consulta_ctp', f.substring(df['value'], 1204, 1).cast(t.IntegerType()))
              .withColumn('ic_veiculo_atu_hst', f.substring(df['value'], 1205, 1).cast(t.IntegerType()))
              .withColumn('ic_veiculo_fin', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1206, 1))))
              .withColumn('ic_fin_ativ_qui', f.substring(df['value'], 1207, 1).cast(t.IntegerType()))
              .withColumn('vl_veiculo_fin', f.substring(df['value'], 1208, 6).cast(t.IntegerType()))
              .withColumn('cd_cnpj_age_fin', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1214, 4))))
              .withColumn('qt_prazo1_decorr', f.substring(df['value'], 1218, 3).cast(t.IntegerType()))
              .withColumn('ic_veiculo2_atu_hst', f.substring(df['value'], 1221, 1).cast(t.IntegerType()))
              .withColumn('ic_veiculo2_fin', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1222, 1))))
              .withColumn('ic_fin2_ativ_qui', f.substring(df['value'], 1223, 1).cast(t.IntegerType()))
              .withColumn('qt_prazo2_decorr', f.substring(df['value'], 1224, 3).cast(t.IntegerType()))
              .withColumn('ic_veiculo3_atu_hst', f.substring(df['value'], 1227, 1).cast(t.IntegerType()))
              .withColumn('ic_veiculo3_fin', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1228, 1))))
              .withColumn('ic_fin3_ativ_qui', f.substring(df['value'], 1229, 1).cast(t.IntegerType()))
              .withColumn('qt_prazo3_decorr', f.substring(df['value'], 1230, 3).cast(t.IntegerType()))
              .withColumn('id_media_veiculo_adq', f.substring(df['value'], 1233, 2).cast(t.IntegerType()))
              .withColumn('csd3_score_pj', f.substring(df['value'], 1235, 6).cast(t.IntegerType()))
              .withColumn('csd3_msg', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1241, 6))))
              .withColumn('csd3_status', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1247, 1))))
              .withColumn('filler', ut_f.limpa_espaco_branco(f.upper(f.substring(df['value'], 1248, 153))))
              .withColumn('dt_carga',f.lit(str(_nameDateFile)))
              #.withColumn('dt_carga',
              #            (f.from_unixtime(f.unix_timestamp(f.current_date(), 'ddMMyyyy'), 'yyyyMMdd')).cast(
              #                t.StringType()))
              #   .withColumn('dt_carga',f.lit('20190619').cast(t.StringType()))
              )
    return result.drop('value')


def lookup_ocupacao(df, lookup_ocupacao):
    # Lista com as colunas do df original e descrição logo após a coluna do codigo
    colunas = df.columns
    colunas.insert(colunas.index('cd_ocupacao') + 1, "PARA")
    lookup_ocupacao = lookup_ocupacao.withColumnRenamed('CD_PROFISSAO', 'DE').withColumnRenamed('DS_PROFISSAO',
                                                                                                'PARA')
    return df \
        .join(lookup_ocupacao, df['cd_ocupacao'] == lookup_ocupacao['DE'], 'left') \
        .select(colunas) \
        .withColumnRenamed('PARA', 'ds_ocupacao')


def lookup_ocupacao_conjuge(df, lookup_ocupacao):
    # Lista com as colunas do df original e descrição logo após a coluna do codigo
    colunas = df.columns
    colunas.insert(colunas.index('cd_ocupacao_conjuge') + 1, "PARA")
    lookup_ocupacao = lookup_ocupacao.withColumnRenamed('CD_PROFISSAO', 'DE').withColumnRenamed('DS_PROFISSAO',
                                                                                                'PARA')

    return df \
        .join(lookup_ocupacao, df['cd_ocupacao'] == lookup_ocupacao['DE'], 'left') \
        .select(colunas) \
        .withColumnRenamed('PARA', 'ds_ocupacao_conjuge')


def lookup_profissao(df, lookup_profissao):
    # Lista com as colunas do df original e descrição logo após a coluna do codigo
    colunas = df.columns
    colunas.insert(colunas.index('cd_profissao') + 1, "PARA")
    lookup_profissao = lookup_profissao.withColumnRenamed('CD_PROFISSAO', 'DE').withColumnRenamed('DS_PROFISSAO',
                                                                                                  'PARA')

    return df \
        .join(lookup_profissao, df['cd_profissao'] == lookup_profissao['DE'], 'left') \
        .select(colunas) \
        .withColumnRenamed('PARA', 'ds_profissao_lookup')


def lookup_profissao_conjuge(df, lookup_profissao):
    # Lista com as colunas do df original e descrição logo após a coluna do codigo
    colunas = df.columns
    colunas.insert(colunas.index('cd_profissao_conjuge') + 1, "PARA")
    lookup_profissao = lookup_profissao.withColumnRenamed('CD_PROFISSAO', 'DE').withColumnRenamed('DS_PROFISSAO',
                                                                                                  'PARA')

    return df \
        .join(lookup_profissao, df['cd_profissao_conjuge'] == lookup_profissao['DE'], 'left') \
        .select(colunas) \
        .withColumnRenamed('PARA', 'ds_profissao_conjuge_lookup')


def gera_colunas(df, colunas_inicio_tamanho):
    for coluna in colunas_inicio_tamanho:
        df = df.withColumn(coluna[0], f.substring(df['value'], coluna[1][0], coluna[1][1]))
    return df.drop(df['value'])



df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)

_dados_io = DadosIO('RED_PROPOSTAS_CLIENTES')
_gerenciador = Gerenciador(_dados_io)
df_propostas_fin_cli = _gerenciador.gera_regras()

try:
    tabela_hive = 'harmonized.h_financeira_propostas_clientes'
    _dados_io.gravar_parquet(df_propostas_fin_cli, tabela_hive)
except Exception as ex:
    df3 = 'ERRO: {}'.format(str(ex))
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)
    ut_f.gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    # exit(0)
    exit(1)
else:
    df3 = ("succeeded")
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)
    ut_f.gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    print 'gravado'
    print 'fim'
    exit(0)
    # print("fim processo")
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
import traceback
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

processo = sys.argv[1]

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

    def dados_parcelas(self):
        # data / harmonized / Financeira / Parcelas / diario / 20190612 / FINB5361 /
        # URL = "C:/hdp_dev/development/source/RED/arquivo/parcelas/FIN_PARCELA_20190325.TXT"
        URL = '/data/harmonized/Financeira/Parcelas/diario/FINB536/' + str(
            datetime.today().strftime('%Y%m%d')) + '/FINB5361/'
        return DadosIO._spark.read.text(URL)

    def proposta_clientes(self):
        return DadosIO._spark.read.text(
            '/data/harmonized/Financeira/ADP/diario/ADPB05N/{}/ADPB05N2/'.format(
                str(datetime.today().strftime('%Y%m%d'))))
        #  '/data/harmonized/Financeira/ADP/diario/ADPB05N/20190619/ADPB05N3/')

    def df_objeto_financiado(self):
        return DadosIO._spark.read.csv('abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/Financeira/Lookup/objeto_financiado/', header=True, sep=';')

    # def lookup_ocupacao(self):
    #     return DadosIO._spark.read \
    #         .format('com.databricks.spark.csv') \
    #         .option('header', 'true') \
    #         .option('delimiter', ',') \
    #         .option('inferSchema', 'true') \
    #         .load('/data/raw/Lookup/objeto_ocupacao/')

    def df_produtos(self):
        return DadosIO._spark.read.csv('abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/Financeira/Lookup/codigo_produto/', header=True, sep=';')

    # def lookup_profissao(self):
    #     return DadosIO._spark.read \
    #         .format('com.databricks.spark.csv') \
    #         .option('header', 'true') \
    #         .option('delimiter', ',') \
    #         .option('inferSchema', 'true') \
    #         .load('/data/raw/Lookup/objeto_profissao/')

    # def objeto_financiado(self):
    #     return DadosIO._spark.read \
    #         .format('com.databricks.spark.csv') \
    #         .option('header', 'true') \
    #         .option('delimiter', ';') \
    #         .option('inferSchema', 'true') \
    #         .load('/data/raw/Lookup/Objeto_financiado/')

#    def gravar_parquet(self, df, nome):
#        df.write\
#            .option("encoding", "UTF-8")\
#            .format("parquet")\
#            .option("header", "false")\
#            .insertInto(nome, overwrite=True)
    def gravar_parquet(self, df, nome):
            df.write.option("encoding", "UTF-8") \
                .mode("overwrite") \
                .format("parquet") \
                .option("header", "false") \
                .partitionBy('dt_carga') \
                .saveAsTable(nome, path='data/harmonized/Financeira/ADP/')

    def path_arquivos(self):
        URL = '/data/harmonized/Financeira/ADP/diario/ADPB05N/{}/ADPB05N2/'.format(str(datetime.today().strftime('%Y%m%d')))
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
        self._objeto_financiado = _dados_io.df_objeto_financiado()
        self._produtos = _dados_io.df_produtos()

    # chamada de todos os passos da ingestao
    def gera_regras(self):
        # ----------------------------------------------
        _urlFile = self._dados_io.path_arquivos()
        print "_urlFile {}".format(_urlFile)
        _nameDateFile = self._dados_io.retirarDataNameFile(_urlFile, "basepropostas_")
        print "_nameDateFile {}".format(_nameDateFile)

        dados = _dados_io.carregarArquivo(_urlFile)
        # ----------------------------------------------
        dados.show(10)
        #ut_f.debug('fim 51')
        result = propostas(dados)
        arquivo_tratado = df_trata_propostas(result, _nameDateFile)
        lk_propostas = df_lk_propostas(arquivo_tratado, self._objeto_financiado, self._produtos)
        final = df_final(lk_propostas)
        return final


#  Função que retorna os dados do arquivo de PARCELAS e realiza o tratamento para quebra de colunas.
# layout final do arquivo
df_layout_propostas = ['tp_registro',
                       'cd_registro',
                       'cd_agencia',
                       'cd_digitador',
                       'cd_intermediario',
                       'cd_operador',
                       'cd_subsegmento',
                       'cd_captacao',
                       'cd_sistema',
                       'cd_proposta',
                       'dt_inclusao',
                       'hr_inclusao',
                       'no_contrato',
                       'cd_produto',
                       'ds_produto',
                       'cd_classificacao_filial',
                       'cd_modelo_score1',
                       'cd_modelo_score2',
                       'cd_modelo_score3',
                       'no_score_credito1',
                       'no_score_credito2',
                       'no_score_credito3',
                       'dt_escoragem',
                       'hr_escoragem',
                       'st_proposta_scs',
                       'st_proposta_final',
                       'dt_status',
                       'hr_status',
                       'posic_politica',
                       'cd_aprovador',
                       'ic_represe_inter',
                       'cd_forma_pagamento',
                       'cd_forma_modal_calculado',
                       'cd_moeda',
                       'vl_total_garantia1',
                       'ds_garantia1',
                       'aa_modelo_garantia1',
                       'cd_modelo_garantia1',
                       'vl_total_garantia2',
                       'ds_garantia2',
                       'aa_modelo_garantia2',
                       'cd_modelo_garantia2',
                       'vl_total_garantia3',
                       'ds_garantia3',
                       'aa_modelo_garantia3',
                       'cd_modelo_garantia3',
                       'qt_garantia',
                       'ic_zero_km',
                       'cd_objeto_financiado',
                       'ds_objeto',
                       'no_contrato_origem_lc',
                       'cd_sistema_origem_lc',
                       'dt_emissao',
                       'vl_principal',
                       'vl_iof',
                       'vl_prestacao_servicos',
                       'vl_entrada',
                       'vl_tarifa_cadastro',
                       'qt_prestacao',
                       'qt_cart_adic',
                       'dt_pagamento_anuidade',
                       'vl_anuidade',
                       'vl_limite',
                       'cd_usuario_tm',
                       'ind_decurso_prazo',
                       'cd_aprovacao',
                       'ic_canal_cap',
                       'qt_repetc_cpf',
                       'qt_reptc_cpf_data_nascimento',
                       'qt_reptc_cpf_loja',
                       'qt_reptc_cpf_loja_plano',
                       'qt_reptc_cpf_loja_plano_prest',
                       'cd_pessoa_pontuada',
                       'vl_veiculo_mercado',
                       'pc_dif_veiculo',
                       'ic_procedencia_veiculo',
                       'ic_taxi',
                       'ic_veiculo_adaptado',
                       'pc_entrada_veiculo',
                       'ic_complemento',
                       'qt_dias_complemento',
                       'qt_reescoragem',
                       'cd_status',
                       'cd_emp',
                       'vl_subsidio_mont',
                       'vl_subsidio_conc',
                       'pc_total_subsidio',
                       'tp_status_ch',
                       'cd_tabela_atx',
                       'cd_comissao',
                       'no_chassi',
                       'cd_central',
                       'cd_time',
                       'dt_primeiro_vencimento',
                       'cd_excessao',
                       'cd_usuario_aprovacao2',
                       'dt_fax',
                       'hr_fax',
                       'st_fila',
                       'vl_taxa_mensal',
                       'vl_taxa_mensal_cliente',
                       'inc_manual',
                       'tx_cet_anual',
                       'tx_cet_mensal',
                       'coeficiente_cet',
                       'cd_tarifa_ren',
                       'vl_comissao_ret',
                       'vl_devolucao_tac',
                       'vl_desp_gravame',
                       'vl_desp_carnet',
                       'vl_desp_cartorio',
                       'cd_just_manual',
                       'vl_tarifa_aval',
                       'cd_justificativa',
                       'cd_categoria',
                       'vl_risco_consolidado_parcela',
                       'vl_total_limite_parcela',
                       'vl_total_limite_rotativo',
                       'cd_fipe1',
                       'cd_fipe2',
                       'cd_fipe3',
                       'dt_entrada_viagem',
                       'pc_comissao',
                       'ic_cart_detran',
                       'cd_pct_liber',
                       'cd_cnl_orn',
                       'cd_pacote_plano',
                       'ic_isenc_tc',
                       'ic_isenc_tab',
                       'score_fraude',
                       'ic_fraude',
                       'cd_atv_ecn_std',
                       'cd_cat_aym',
                       'vl_risco_consolidado_prest',
                       'vl_risco_consolidado_saldo_devedor',
                       'vl_risco_consolidado_rotativo',
                       'perc_comprometimento_renda_decl',
                       'perc_comprometimento_renda_presumida',
                       'perc_contratos_atraso',
                       'qt_dias_maior_atraso',
                       'ic_status_cliente_contrato',
                       'pc_comprometimento_renda_declaracao_familiar1',
                       'pc_compromentimento_renda_presumida_familiar1',
                       'pc_parcela_familiar1',
                       'sld_devedor_familiar1',
                       'limite_familiar1',
                       'pc_compromentimento_renda_declarada_familiar2',
                       'pc_compromentimento_renda_presumida_familiar2',
                       'pc_parcela_familiar2',
                       'sld_devedor_familiar2',
                       'limite_familiar2',
                       'pc_comprometimento_renda_declaracao_familiar3',
                       'pc_comprometimento_renda_presumida_familiar3',
                       'pc_parcela_familiar3',
                       'sld_devedor_familiar3',
                       'limite_familiar3',
                       'ic_tac_vista',
                       'ic_tab_vista',
                       'cd_aget_cerf',
                       'cd_cerf_aget',
                       'filler',
                       'dt_carga']


# dataframe para posicionamento dos campos de propostas
def propostas(_propostas):
    return (_propostas
            .withColumn('tp_registro', f.substring(_propostas['value'], 1, 1))
            .withColumn('cd_registro', f.substring(_propostas['value'], 2, 1))
            .withColumn('cd_agencia', f.substring(_propostas['value'], 3, 3))
            .withColumn('cd_digitador', f.substring(_propostas['value'], 6, 10))
            .withColumn('cd_intermediario', f.substring(_propostas['value'], 16, 6))
            .withColumn('cd_operador', f.substring(_propostas['value'], 22, 5))
            .withColumn('cd_subsegmento', f.substring(_propostas['value'], 27, 3))
            .withColumn('cd_captacao', f.substring(_propostas['value'], 30, 3))
            .withColumn('cd_sistema', f.substring(_propostas['value'], 33, 3))
            .withColumn('cd_proposta', f.substring(_propostas['value'], 36, 10))
            .withColumn('dt_inclusao', f.substring(_propostas['value'], 46, 8))
            .withColumn('hr_inclusao', f.substring(_propostas['value'], 54, 4))
            .withColumn('no_contrato', f.substring(_propostas['value'], 58, 16))
            .withColumn('cd_produto', f.substring(_propostas['value'], 74, 4))
            .withColumn('cd_classificacao_filial', f.substring(_propostas['value'], 78, 1))
            .withColumn('cd_modelo_score1', f.substring(_propostas['value'], 79, 5))
            .withColumn('cd_modelo_score2', f.substring(_propostas['value'], 84, 5))
            .withColumn('cd_modelo_score3', f.substring(_propostas['value'], 89, 5))
            .withColumn('no_score_credito1', f.substring(_propostas['value'], 94, 5))
            .withColumn('no_score_credito2', f.substring(_propostas['value'], 99, 5))
            .withColumn('no_score_credito3', f.substring(_propostas['value'], 104, 5))
            .withColumn('dt_escoragem', f.substring(_propostas['value'], 109, 8))
            .withColumn('hr_escoragem', f.substring(_propostas['value'], 117, 4))
            .withColumn('st_proposta_scs', f.substring(_propostas['value'], 121, 5))
            .withColumn('st_proposta_final', f.substring(_propostas['value'], 126, 5))
            .withColumn('dt_status', f.substring(_propostas['value'], 131, 8))
            .withColumn('hr_status', f.substring(_propostas['value'], 139, 4))
            .withColumn('posic_politica', f.substring(_propostas['value'], 143, 5))
            .withColumn('cd_aprovador', f.substring(_propostas['value'], 148, 10))
            .withColumn('ic_represe_inter', f.substring(_propostas['value'], 158, 1))
            .withColumn('cd_forma_pagamento', f.substring(_propostas['value'], 159, 1))
            .withColumn('cd_forma_modal_calculado', f.substring(_propostas['value'], 160, 1))
            .withColumn('cd_moeda', f.substring(_propostas['value'], 161, 3))
            .withColumn('vl_total_garantia1', f.concat(f.substring(_propostas['value'], 164, 15), f.lit('.'),
                                                       f.substring(_propostas['value'], 179, 2)))
            .withColumn('ds_garantia1', f.substring(_propostas['value'], 181, 30))
            .withColumn('aa_modelo_garantia1', f.substring(_propostas['value'], 211, 4))
            .withColumn('cd_modelo_garantia1', f.substring(_propostas['value'], 215, 4))
            .withColumn('vl_total_garantia2', f.concat(f.substring(_propostas['value'], 219, 15), f.lit('.'),
                                                       f.substring(_propostas['value'], 234, 2)))
            .withColumn('ds_garantia2', f.substring(_propostas['value'], 236, 30))
            .withColumn('aa_modelo_garantia2', f.substring(_propostas['value'], 266, 4))
            .withColumn('cd_modelo_garantia2', f.substring(_propostas['value'], 270, 4))
            .withColumn('vl_total_garantia3', f.concat(f.substring(_propostas['value'], 274, 15), f.lit('.'),
                                                       f.substring(_propostas['value'], 289, 2)))
            .withColumn('ds_garantia3', f.substring(_propostas['value'], 291, 30))
            .withColumn('aa_modelo_garantia3', f.substring(_propostas['value'], 321, 4))
            .withColumn('cd_modelo_garantia3', f.substring(_propostas['value'], 325, 4))
            .withColumn('qt_garantia', f.substring(_propostas['value'], 329, 3))
            .withColumn('ic_zero_km', f.substring(_propostas['value'], 332, 1))
            .withColumn('cd_objeto_financiado', f.substring(_propostas['value'], 333, 2))
            .withColumn('no_contrato_origem_lc', f.substring(_propostas['value'], 335, 16))
            .withColumn('cd_sistema_origem_lc', f.substring(_propostas['value'], 351, 3))
            .withColumn('dt_emissao', f.substring(_propostas['value'], 354, 8))
            .withColumn('vl_principal', f.concat(f.substring(_propostas['value'], 362, 15), f.lit('.'),
                                                 f.substring(_propostas['value'], 377, 2)))
            .withColumn('vl_iof', f.concat(f.substring(_propostas['value'], 379, 15), f.lit('.'),
                                           f.substring(_propostas['value'], 394, 2)))
            .withColumn('vl_prestacao_servicos', f.concat(f.substring(_propostas['value'], 396, 15), f.lit('.'),
                                                          f.substring(_propostas['value'], 411, 2)))
            .withColumn('vl_entrada', f.concat(f.substring(_propostas['value'], 413, 15), f.lit('.'),
                                               f.substring(_propostas['value'], 428, 2)))
            .withColumn('vl_tarifa_cadastro', f.concat(f.substring(_propostas['value'], 430, 15), f.lit('.'),
                                                       f.substring(_propostas['value'], 445, 2)))
            .withColumn('qt_prestacao', f.substring(_propostas['value'], 447, 3))
            .withColumn('qt_cart_adic', f.substring(_propostas['value'], 450, 2))
            .withColumn('dt_pagamento_anuidade', f.substring(_propostas['value'], 452, 8))
            .withColumn('vl_anuidade', f.concat(f.substring(_propostas['value'], 460, 15), f.lit('.'),
                                                f.substring(_propostas['value'], 475, 2)))
            .withColumn('vl_limite', f.concat(f.substring(_propostas['value'], 477, 15), f.lit('.'),
                                              f.substring(_propostas['value'], 492, 2)))
            .withColumn('cd_usuario_tm', f.substring(_propostas['value'], 494, 10))
            .withColumn('ind_decurso_prazo', f.substring(_propostas['value'], 504, 1))
            .withColumn('cd_aprovacao', f.substring(_propostas['value'], 505, 11))
            .withColumn('ic_canal_cap', f.substring(_propostas['value'], 516, 2))
            .withColumn('qt_repetc_cpf', f.substring(_propostas['value'], 518, 2))
            .withColumn('qt_reptc_cpf_data_nascimento', f.substring(_propostas['value'], 520, 2))
            .withColumn('qt_reptc_cpf_loja', f.substring(_propostas['value'], 522, 2))
            .withColumn('qt_reptc_cpf_loja_plano', f.substring(_propostas['value'], 524, 2))
            .withColumn('qt_reptc_cpf_loja_plano_prest', f.substring(_propostas['value'], 526, 2))
            .withColumn('cd_pessoa_pontuada', f.substring(_propostas['value'], 528, 2))
            .withColumn('vl_veiculo_mercado', f.concat(f.substring(_propostas['value'], 530, 15), f.lit('.'),
                                                       f.substring(_propostas['value'], 545, 2)))
            .withColumn('pc_dif_veiculo', f.concat(f.substring(_propostas['value'], 547, 4), f.lit('.'),
                                                   f.substring(_propostas['value'], 552, 1)))
            .withColumn('ic_procedencia_veiculo', f.substring(_propostas['value'], 552, 1))
            .withColumn('ic_taxi', f.substring(_propostas['value'], 553, 1))
            .withColumn('ic_veiculo_adaptado', f.substring(_propostas['value'], 554, 1))
            .withColumn('pc_entrada_veiculo', f.substring(_propostas['value'], 555, 9))
            .withColumn('ic_complemento', f.substring(_propostas['value'], 564, 1))
            .withColumn('qt_dias_complemento', f.substring(_propostas['value'], 565, 3))
            .withColumn('qt_reescoragem', f.substring(_propostas['value'], 568, 2))
            .withColumn('cd_status', f.substring(_propostas['value'], 570, 2))
            .withColumn('cd_emp', f.substring(_propostas['value'], 572, 3))
            .withColumn('vl_subsidio_mont', f.concat(f.substring(_propostas['value'], 575, 13), f.lit('.'),
                                                     f.substring(_propostas['value'], 588, 2)))
            .withColumn('vl_subsidio_conc', f.concat(f.substring(_propostas['value'], 590, 13), f.lit('.'),
                                                     f.substring(_propostas['value'], 603, 2)))
            .withColumn('pc_total_subsidio', f.substring(_propostas['value'], 605, 5))
            .withColumn('tp_status_ch', f.substring(_propostas['value'], 610, 1))
            .withColumn('cd_tabela_atx', f.substring(_propostas['value'], 611, 7))
            .withColumn('cd_comissao', f.substring(_propostas['value'], 618, 2))
            .withColumn('no_chassi', f.substring(_propostas['value'], 620, 20))
            .withColumn('cd_central', f.substring(_propostas['value'], 640, 2))
            .withColumn('cd_time', f.substring(_propostas['value'], 642, 3))
            .withColumn('dt_primeiro_vencimento', f.substring(_propostas['value'], 645, 8))
            .withColumn('cd_excessao', f.substring(_propostas['value'], 653, 3))
            .withColumn('cd_usuario_aprovacao2', f.substring(_propostas['value'], 656, 8))
            .withColumn('dt_fax', f.substring(_propostas['value'], 664, 8))
            .withColumn('hr_fax', f.substring(_propostas['value'], 672, 4))
            .withColumn('st_fila', f.substring(_propostas['value'], 676, 2))
            .withColumn('vl_taxa_mensal', f.concat(f.substring(_propostas['value'], 678, 6), f.lit('.'),
                                                   f.substring(_propostas['value'], 684, 9)))
            .withColumn('vl_taxa_mensal_cliente', f.concat(f.substring(_propostas['value'], 693, 3), f.lit('.'),
                                                           f.substring(_propostas['value'], 696, 6)))
            .withColumn('inc_manual', f.substring(_propostas['value'], 702, 1))
            .withColumn('tx_cet_anual', f.substring(_propostas['value'], 703, 5))
            .withColumn('tx_cet_mensal', f.substring(_propostas['value'], 708, 5))
            .withColumn('coeficiente_cet', f.concat(f.substring(_propostas['value'], 713, 6), f.lit('.'),
                                                    f.substring(_propostas['value'], 719, 6)))
            .withColumn('cd_tarifa_ren', f.substring(_propostas['value'], 725, 2))
            .withColumn('vl_comissao_ret', f.concat(f.substring(_propostas['value'], 727, 11), f.lit('.'),
                                                    f.substring(_propostas['value'], 738, 2)))
            .withColumn('vl_devolucao_tac', f.concat(f.substring(_propostas['value'], 740, 11), f.lit('.'),
                                                     f.substring(_propostas['value'], 751, 2)))
            .withColumn('vl_desp_gravame', f.concat(f.substring(_propostas['value'], 753, 11), f.lit('.'),
                                                    f.substring(_propostas['value'], 764, 2)))
            .withColumn('vl_desp_carnet', f.concat(f.substring(_propostas['value'], 766, 11), f.lit('.'),
                                                   f.substring(_propostas['value'], 777, 2)))
            .withColumn('vl_desp_cartorio', f.concat(f.substring(_propostas['value'], 779, 11), f.lit('.'),
                                                     f.substring(_propostas['value'], 790, 2)))
            .withColumn('cd_just_manual', f.substring(_propostas['value'], 792, 3))
            .withColumn('vl_tarifa_aval', f.concat(f.substring(_propostas['value'], 795, 11), f.lit('.'),
                                                   f.substring(_propostas['value'], 806, 2)))
            .withColumn('cd_justificativa', f.substring(_propostas['value'], 808, 3))
            .withColumn('cd_categoria', f.substring(_propostas['value'], 811, 2))
            .withColumn('vl_risco_consolidado_parcela',
                        f.concat(f.substring(_propostas['value'], 813, 11), f.lit('.'),
                                 f.substring(_propostas['value'], 824, 2)))
            .withColumn('vl_total_limite_parcela', f.concat(f.substring(_propostas['value'], 826, 11), f.lit('.'),
                                                            f.substring(_propostas['value'], 837, 2)))
            .withColumn('vl_total_limite_rotativo', f.concat(f.substring(_propostas['value'], 839, 11), f.lit('.'),
                                                             f.substring(_propostas['value'], 850, 2)))
            .withColumn('cd_fipe1', f.substring(_propostas['value'], 852, 9))
            .withColumn('cd_fipe2', f.substring(_propostas['value'], 861, 9))
            .withColumn('cd_fipe3', f.substring(_propostas['value'], 870, 9))
            .withColumn('dt_entrada_viagem', f.substring(_propostas['value'], 879, 8))
            .withColumn('pc_comissao', f.concat(f.substring(_propostas['value'], 887, 2), f.lit('.'),
                                                f.substring(_propostas['value'], 889, 2)))
            .withColumn('ic_cart_detran', f.substring(_propostas['value'], 891, 1))
            .withColumn('cd_pct_liber', f.substring(_propostas['value'], 892, 7))
            .withColumn('cd_cnl_orn', f.substring(_propostas['value'], 899, 4))
            .withColumn('cd_pacote_plano', f.substring(_propostas['value'], 903, 6))
            .withColumn('ic_isenc_tc', f.substring(_propostas['value'], 909, 1))
            .withColumn('ic_isenc_tab', f.substring(_propostas['value'], 910, 1))
            .withColumn('score_fraude', f.substring(_propostas['value'], 911, 7))
            .withColumn('ic_fraude', f.substring(_propostas['value'], 918, 1))
            .withColumn('cd_atv_ecn_std', f.substring(_propostas['value'], 919, 7))
            .withColumn('cd_cat_aym', f.substring(_propostas['value'], 926, 1))
            .withColumn('vl_risco_consolidado_prest',
                        f.concat(f.substring(_propostas['value'], 927, 13), f.lit('.'),
                                 f.substring(_propostas['value'], 938, 2)))
            .withColumn('vl_risco_consolidado_saldo_devedor',
                        f.concat(f.substring(_propostas['value'], 942, 13), f.lit('.'),
                                 f.substring(_propostas['value'], 955, 2)))
            .withColumn('vl_risco_consolidado_rotativo',
                        f.concat(f.substring(_propostas['value'], 957, 13), f.lit('.'),
                                 f.substring(_propostas['value'], 970, 2)))
            .withColumn('perc_comprometimento_renda_decl',
                        f.concat(f.substring(_propostas['value'], 972, 4), f.lit('.'),
                                 f.substring(_propostas['value'], 976, 2)))
            .withColumn('perc_comprometimento_renda_presumida',
                        f.concat(f.substring(_propostas['value'], 978, 4), f.lit('.'),
                                 f.substring(_propostas['value'], 982, 2)))
            .withColumn('perc_contratos_atraso', f.concat(f.substring(_propostas['value'], 984, 4), f.lit('.'),
                                                          f.substring(_propostas['value'], 988, 2)))
            .withColumn('qt_dias_maior_atraso', f.substring(_propostas['value'], 990, 5))
            .withColumn('ic_status_cliente_contrato', f.substring(_propostas['value'], 995, 1))
            .withColumn('pc_comprometimento_renda_declaracao_familiar1', f.substring(_propostas['value'], 996, 3))
            .withColumn('pc_compromentimento_renda_presumida_familiar1', f.substring(_propostas['value'], 999, 3))
            .withColumn('pc_parcela_familiar1', f.substring(_propostas['value'], 1002, 3))
            .withColumn('sld_devedor_familiar1', f.substring(_propostas['value'], 1005, 3))
            .withColumn('limite_familiar1', f.substring(_propostas['value'], 1008, 3))
            .withColumn('pc_compromentimento_renda_declarada_familiar2', f.substring(_propostas['value'], 1011, 3))
            .withColumn('pc_compromentimento_renda_presumida_familiar2', f.substring(_propostas['value'], 1014, 3))
            .withColumn('pc_parcela_familiar2', f.substring(_propostas['value'], 1017, 3))
            .withColumn('sld_devedor_familiar2', f.substring(_propostas['value'], 1020, 3))
            .withColumn('limite_familiar2', f.substring(_propostas['value'], 1023, 3))
            .withColumn('pc_comprometimento_renda_declaracao_familiar3', f.substring(_propostas['value'], 1026, 3))
            .withColumn('pc_comprometimento_renda_presumida_familiar3', f.substring(_propostas['value'], 1029, 3))
            .withColumn('pc_parcela_familiar3', f.substring(_propostas['value'], 1032, 3))
            .withColumn('sld_devedor_familiar3', f.substring(_propostas['value'], 1035, 3))
            .withColumn('limite_familiar3', f.substring(_propostas['value'], 1038, 3))
            .withColumn('ic_tac_vista', f.substring(_propostas['value'], 1041, 1))
            .withColumn('ic_tab_vista', f.substring(_propostas['value'], 1042, 1))
            .withColumn('cd_aget_cerf', f.substring(_propostas['value'], 1043, 12))
            .withColumn('cd_cerf_aget', f.substring(_propostas['value'], 1055, 16))
            .withColumn('filler', f.substring(_propostas['value'], 1071, 330))
            .drop('value')
            )


# dataframe para tratamentos da proposta
def df_trata_propostas(df_propostas, _data_arquivo):
    return (df_propostas
            .withColumn('tp_registro', df_propostas['tp_registro'].cast(IntegerType()))
            .withColumn('cd_registro', df_propostas['cd_registro'].cast(IntegerType()))
            .withColumn('cd_agencia', df_propostas['cd_agencia'].cast(IntegerType()))
            .withColumn('cd_digitador', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_digitador'])))
            .withColumn('cd_intermediario', df_propostas['cd_intermediario'].cast(IntegerType()))
            .withColumn('cd_operador', df_propostas['cd_operador'].cast(IntegerType()))
            .withColumn('cd_subsegmento', df_propostas['cd_subsegmento'].cast(IntegerType()))
            .withColumn('cd_captacao', df_propostas['cd_captacao'].cast(IntegerType()))
            .withColumn('cd_sistema', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_sistema'])))
            .withColumn('cd_proposta', df_propostas['cd_proposta'].cast('bigint'))
            .withColumn('dt_inclusao', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_inclusao'], 'ddMMyyyy'), 'yyyyMMdd').cast(IntegerType())))
            .withColumn('hr_inclusao', df_propostas['hr_inclusao'].cast(IntegerType()))
            .withColumn('no_contrato', df_propostas['no_contrato'].cast('bigint'))
            .withColumn('cd_produto', df_propostas['cd_produto'].cast(IntegerType()))
            .withColumn('cd_classificacao_filial', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_classificacao_filial'])))

            # a confirmar tratamentos
            .withColumn('cd_modelo_score1', ut_f.limpa_espaco_branco(df_propostas['cd_modelo_score1']))
            .withColumn('cd_modelo_score2', ut_f.limpa_espaco_branco(df_propostas['cd_modelo_score2']))
            .withColumn('cd_modelo_score3', ut_f.limpa_espaco_branco(df_propostas['cd_modelo_score3']))
            .withColumn('no_score_credito1', df_propostas['no_score_credito1'].cast(IntegerType()))
            .withColumn('no_score_credito2', df_propostas['no_score_credito2'].cast(IntegerType()))
            .withColumn('no_score_credito3', df_propostas['no_score_credito3'].cast(IntegerType()))

            .withColumn('dt_escoragem', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_escoragem'], 'ddMMyyyy'), 'yyyyMMdd').cast(
            IntegerType())))
            .withColumn('hr_escoragem', df_propostas['hr_escoragem'].cast(IntegerType()))
            .withColumn('st_proposta_scs', ut_f.limpa_espaco_branco(f.upper(df_propostas['st_proposta_scs'])))
            .withColumn('st_proposta_final', ut_f.limpa_espaco_branco(f.upper(df_propostas['st_proposta_final'])))
            .withColumn('dt_status', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_status'], 'ddMMyyyy'), 'yyyyMMdd').cast(IntegerType())))
            .withColumn('hr_status', df_propostas['hr_status'].cast(IntegerType()))
            .withColumn('posic_politica', df_propostas['posic_politica'].cast(IntegerType()))
            .withColumn('cd_aprovador', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_aprovador'])))
            .withColumn('ic_represe_inter', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_represe_inter'])))

            # a confirmar tratamentos
            .withColumn('cd_forma_pagamento', df_propostas['cd_forma_pagamento'].cast(IntegerType()))
            .withColumn('cd_forma_modal_calculado', df_propostas['cd_forma_modal_calculado'].cast(IntegerType()))
            .withColumn('cd_moeda', df_propostas['cd_moeda'].cast(IntegerType()))

            .withColumn('vl_total_garantia1', df_propostas['vl_total_garantia1'].cast(DecimalType(15, 2)))
            .withColumn('ds_garantia1', ut_f.limpa_espaco_branco(f.upper(df_propostas['ds_garantia1'])))
            .withColumn('aa_modelo_garantia1', df_propostas['aa_modelo_garantia1'].cast(IntegerType()))
            .withColumn('cd_modelo_garantia1', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_modelo_garantia1'])))
            .withColumn('vl_total_garantia2', df_propostas['vl_total_garantia2'].cast(DecimalType(15, 2)))
            .withColumn('ds_garantia2', ut_f.limpa_espaco_branco(f.upper(df_propostas['ds_garantia2'])))
            .withColumn('aa_modelo_garantia2', df_propostas['aa_modelo_garantia2'].cast(IntegerType()))
            .withColumn('cd_modelo_garantia2', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_modelo_garantia2'])))
            .withColumn('vl_total_garantia3', df_propostas['vl_total_garantia3'].cast(DecimalType(15, 2)))
            .withColumn('ds_garantia3', ut_f.limpa_espaco_branco(f.upper(df_propostas['ds_garantia3'])))
            .withColumn('aa_modelo_garantia3', df_propostas['aa_modelo_garantia3'].cast(IntegerType()))
            .withColumn('cd_modelo_garantia3', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_modelo_garantia3'])))
            .withColumn('qt_garantia', df_propostas['qt_garantia'].cast(IntegerType()))
            .withColumn('ic_zero_km', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_zero_km'])))
            .withColumn('cd_objeto_financiado', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_objeto_financiado'])))
            .withColumn('no_contrato_origem_lc', df_propostas['no_contrato_origem_lc'].cast(IntegerType()))
            .withColumn('cd_sistema_origem_lc', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_sistema_origem_lc'])))
            .withColumn('dt_emissao', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_emissao'], 'ddMMyyyy'), 'yyyyMMdd').cast(IntegerType())))
            .withColumn('vl_principal', df_propostas['vl_principal'].cast(DecimalType(15, 2)))
            .withColumn('vl_iof', df_propostas['vl_iof'].cast(DecimalType(15, 2)))
            .withColumn('vl_prestacao_servicos', df_propostas['vl_prestacao_servicos'].cast(DecimalType(15, 2)))
            .withColumn('vl_entrada', df_propostas['vl_entrada'].cast(DecimalType(15, 2)))
            .withColumn('vl_tarifa_cadastro', df_propostas['vl_tarifa_cadastro'].cast(DecimalType(15, 2)))
            .withColumn('qt_prestacao', df_propostas['qt_prestacao'].cast(IntegerType()))
            .withColumn('qt_cart_adic', df_propostas['qt_cart_adic'].cast(IntegerType()))
            .withColumn('dt_pagamento_anuidade', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_pagamento_anuidade'], 'ddMMyyyy'), 'yyyyMMdd').cast(
            IntegerType())))
            .withColumn('vl_anuidade', df_propostas['vl_anuidade'].cast(DecimalType(15, 2)))
            .withColumn('vl_limite', df_propostas['vl_limite'].cast(DecimalType(15, 2)))
            .withColumn('cd_usuario_tm', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_usuario_tm'])))
            .withColumn('ind_decurso_prazo', ut_f.limpa_espaco_branco(f.upper(df_propostas['ind_decurso_prazo'])))
            .withColumn('cd_aprovacao', df_propostas['cd_aprovacao'].cast(IntegerType()))
            .withColumn('ic_canal_cap', df_propostas['ic_canal_cap'].cast(IntegerType()))
            .withColumn('qt_repetc_cpf', df_propostas['qt_repetc_cpf'].cast(IntegerType()))
            .withColumn('qt_reptc_cpf_data_nascimento',
                        df_propostas['qt_reptc_cpf_data_nascimento'].cast(IntegerType()))
            .withColumn('qt_reptc_cpf_loja', df_propostas['qt_reptc_cpf_loja'].cast(IntegerType()))
            .withColumn('qt_reptc_cpf_loja_plano', df_propostas['qt_reptc_cpf_loja_plano'].cast(IntegerType()))
            .withColumn('qt_reptc_cpf_loja_plano_prest',
                        df_propostas['qt_reptc_cpf_loja_plano_prest'].cast(IntegerType()))
            .withColumn('cd_pessoa_pontuada', df_propostas['cd_pessoa_pontuada'].cast(IntegerType()))
            .withColumn('vl_veiculo_mercado', df_propostas['vl_veiculo_mercado'].cast(DecimalType(15, 2)))
            .withColumn('pc_dif_veiculo', df_propostas['pc_dif_veiculo'].cast(DecimalType(4, 1)))
            .withColumn('ic_procedencia_veiculo', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_procedencia_veiculo'])))
            .withColumn('ic_taxi', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_taxi'])))
            .withColumn('ic_veiculo_adaptado', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_veiculo_adaptado'])))
            .withColumn('pc_entrada_veiculo', df_propostas['pc_entrada_veiculo'].cast(DecimalType(7, 2)))
            .withColumn('ic_complemento', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_complemento'])))
            .withColumn('qt_dias_complemento', df_propostas['qt_dias_complemento'].cast(IntegerType()))
            .withColumn('qt_reescoragem', df_propostas['qt_reescoragem'].cast(IntegerType()))
            .withColumn('cd_status', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_status'])))
            .withColumn('cd_emp', df_propostas['cd_emp'].cast(IntegerType()))
            .withColumn('vl_subsidio_mont', df_propostas['vl_subsidio_mont'].cast(DecimalType(13, 2)))
            .withColumn('vl_subsidio_conc', df_propostas['vl_subsidio_conc'].cast(DecimalType(13, 2)))
            .withColumn('pc_total_subsidio', df_propostas['pc_total_subsidio'].cast(DecimalType(3, 2)))

            # a confirmar tratamentos
            .withColumn('tp_status_ch', ut_f.limpa_espaco_branco(f.upper(df_propostas['tp_status_ch'])))

            .withColumn('cd_tabela_atx', df_propostas['cd_tabela_atx'].cast(IntegerType()))
            .withColumn('cd_comissao', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_comissao'])))
            .withColumn('no_chassi', ut_f.limpa_espaco_branco(f.upper(df_propostas['no_chassi'])))
            .withColumn('cd_central', df_propostas['cd_central'].cast(IntegerType()))
            .withColumn('cd_time', df_propostas['cd_time'].cast(IntegerType()))
            .withColumn('dt_primeiro_vencimento', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_primeiro_vencimento'], 'ddMMyyyy'), 'yyyyMMdd').cast(
            IntegerType())))
            .withColumn('cd_excessao', df_propostas['cd_excessao'].cast(IntegerType()))
            .withColumn('cd_usuario_aprovacao2', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_usuario_aprovacao2'])))
            .withColumn('dt_fax',
                        (f.from_unixtime(f.unix_timestamp(df_propostas['dt_fax'], 'ddMMyyyy'), 'yyyyMMdd')).cast(
                            IntegerType()))
            .withColumn('hr_fax', df_propostas['hr_fax'].cast(IntegerType()))

            # a confirmar tratamentos
            .withColumn('st_fila', ut_f.limpa_espaco_branco(f.upper(df_propostas['st_fila'])))

            .withColumn('vl_taxa_mensal',
                        df_propostas['vl_taxa_mensal'].cast(DecimalType(15, 9)).cast(StringType()))
            .withColumn('vl_taxa_mensal_cliente',
                        df_propostas['vl_taxa_mensal_cliente'].cast(DecimalType(9, 6)).cast(StringType()))

            .withColumn('inc_manual', ut_f.limpa_espaco_branco(f.upper(df_propostas['inc_manual'])))
            .withColumn('tx_cet_anual', df_propostas['tx_cet_anual'].cast(DecimalType(3, 2)))
            .withColumn('tx_cet_mensal', df_propostas['tx_cet_mensal'].cast(DecimalType(3, 2)))
            .withColumn('coeficiente_cet',
                        df_propostas['coeficiente_cet'].cast(DecimalType(12, 6)).cast(StringType()))
            .withColumn('cd_tarifa_ren', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_tarifa_ren'])))
            .withColumn('vl_comissao_ret', df_propostas['vl_comissao_ret'].cast(DecimalType(11, 2)))
            .withColumn('vl_devolucao_tac', df_propostas['vl_devolucao_tac'].cast(DecimalType(11, 2)))
            .withColumn('vl_desp_gravame', df_propostas['vl_desp_gravame'].cast(DecimalType(11, 2)))
            .withColumn('vl_desp_carnet', df_propostas['vl_desp_carnet'].cast(DecimalType(11, 2)))
            .withColumn('vl_desp_cartorio', df_propostas['vl_desp_cartorio'].cast(DecimalType(11, 2)))
            .withColumn('cd_just_manual', df_propostas['cd_just_manual'].cast(IntegerType()))
            .withColumn('vl_tarifa_aval', df_propostas['vl_tarifa_aval'].cast(DecimalType(11, 2)))
            .withColumn('cd_justificativa', df_propostas['cd_justificativa'].cast(IntegerType()))
            .withColumn('cd_categoria', df_propostas['cd_categoria'].cast(IntegerType()))
            .withColumn('vl_risco_consolidado_parcela',
                        df_propostas['vl_risco_consolidado_parcela'].cast(DecimalType(11, 2)))
            .withColumn('vl_total_limite_parcela', df_propostas['vl_total_limite_parcela'].cast(DecimalType(11, 2)))
            .withColumn('vl_total_limite_rotativo',
                        df_propostas['vl_total_limite_rotativo'].cast(DecimalType(11, 2)))
            .withColumn('cd_fipe1', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_fipe1'])))
            .withColumn('cd_fipe2', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_fipe2'])))
            .withColumn('cd_fipe3', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_fipe3'])))
            .withColumn('dt_entrada_viagem', (
        f.from_unixtime(f.unix_timestamp(df_propostas['dt_entrada_viagem'], 'ddMMyyyy'), 'yyyyMMdd').cast(
            IntegerType())))
            .withColumn('pc_comissao', df_propostas['pc_comissao'].cast(DecimalType(2, 2)))

            # a confirmar tratamentos
            .withColumn('ic_cart_detran', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_cart_detran'])))
            .withColumn('cd_pct_liber', df_propostas['cd_pct_liber'].cast(IntegerType()))

            .withColumn('cd_cnl_orn', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_cnl_orn'])))

            # a confirmar tratamentos
            .withColumn('cd_pacote_plano', df_propostas['cd_pacote_plano'].cast(IntegerType()))

            .withColumn('ic_isenc_tc', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_isenc_tc'])))
            .withColumn('ic_isenc_tab', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_isenc_tab'])))
            .withColumn('score_fraude', df_propostas['score_fraude'].cast(IntegerType()))
            .withColumn('ic_fraude', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_fraude'])))
            .withColumn('cd_atv_ecn_std', df_propostas['cd_atv_ecn_std'].cast(IntegerType()))
            .withColumn('cd_cat_aym', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_cat_aym'])))
            .withColumn('vl_risco_consolidado_prest',
                        df_propostas['vl_risco_consolidado_prest'].cast(DecimalType(13, 2)))
            .withColumn('vl_risco_consolidado_saldo_devedor',
                        df_propostas['vl_risco_consolidado_saldo_devedor'].cast(DecimalType(13, 2)))
            .withColumn('vl_risco_consolidado_rotativo',
                        df_propostas['vl_risco_consolidado_rotativo'].cast(DecimalType(13, 2)))
            .withColumn('perc_comprometimento_renda_decl',
                        df_propostas['perc_comprometimento_renda_decl'].cast(DecimalType(4, 2)))
            .withColumn('perc_comprometimento_renda_presumida',
                        df_propostas['perc_comprometimento_renda_presumida'].cast(DecimalType(4, 2)))
            .withColumn('perc_contratos_atraso', df_propostas['perc_contratos_atraso'].cast(DecimalType(4, 2)))
            .withColumn('qt_dias_maior_atraso', df_propostas['qt_dias_maior_atraso'].cast(IntegerType()))
            .withColumn('ic_status_cliente_contrato', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_status_cliente_contrato'])))
            .withColumn('pc_comprometimento_renda_declaracao_familiar1',
                        df_propostas['pc_comprometimento_renda_declaracao_familiar1'].cast(IntegerType()))
            .withColumn('pc_compromentimento_renda_presumida_familiar1',
                        df_propostas['pc_compromentimento_renda_presumida_familiar1'].cast(IntegerType()))
            .withColumn('pc_parcela_familiar1', df_propostas['pc_parcela_familiar1'].cast(IntegerType()))
            .withColumn('sld_devedor_familiar1', df_propostas['sld_devedor_familiar1'].cast(IntegerType()))
            .withColumn('limite_familiar1', df_propostas['limite_familiar1'].cast(IntegerType()))
            .withColumn('pc_compromentimento_renda_declarada_familiar2',
                        df_propostas['pc_compromentimento_renda_declarada_familiar2'].cast(IntegerType()))
            .withColumn('pc_compromentimento_renda_presumida_familiar2',
                        df_propostas['pc_compromentimento_renda_presumida_familiar2'].cast(IntegerType()))
            .withColumn('pc_parcela_familiar2', df_propostas['pc_parcela_familiar2'].cast(IntegerType()))
            .withColumn('sld_devedor_familiar2', df_propostas['sld_devedor_familiar2'].cast(IntegerType()))
            .withColumn('limite_familiar2', df_propostas['limite_familiar2'].cast(IntegerType()))
            .withColumn('pc_comprometimento_renda_declaracao_familiar3',
                        df_propostas['pc_comprometimento_renda_declaracao_familiar3'].cast(IntegerType()))
            .withColumn('pc_comprometimento_renda_presumida_familiar3',
                        df_propostas['pc_comprometimento_renda_presumida_familiar3'].cast(IntegerType()))
            .withColumn('pc_parcela_familiar3', df_propostas['pc_parcela_familiar3'].cast(IntegerType()))
            .withColumn('sld_devedor_familiar3', df_propostas['sld_devedor_familiar3'].cast(IntegerType()))
            .withColumn('limite_familiar3', df_propostas['limite_familiar3'].cast(IntegerType()))
            .withColumn('ic_tac_vista', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_tac_vista'])))
            .withColumn('ic_tab_vista', ut_f.limpa_espaco_branco(f.upper(df_propostas['ic_tab_vista'])))
            .withColumn('cd_aget_cerf', df_propostas['cd_aget_cerf'].cast(IntegerType()))
            .withColumn('cd_cerf_aget', ut_f.limpa_espaco_branco(f.upper(df_propostas['cd_cerf_aget'])))
            .withColumn('filler', ut_f.limpa_espaco_branco(df_propostas['filler']))
            .withColumn('dt_carga', f.lit(str(_data_arquivo)))
            # .withColumn('dt_carga', (f.from_unixtime(f.unix_timestamp(f.current_date(), 'ddMMyyyy'), 'yyyyMMdd')).cast(                                StringType()))
            # .withColumn('dt_carga',f.lit('20190619').cast(StringType()))
            )


# dataframe lookup objeto_financiado
def df_objeto_financiado(df_h_lk_objeto_financiado):
    return (df_h_lk_objeto_financiado
            .select(f.upper(f.trim('sg_objeto')), f.upper('ds_objeto'))
            )


# data frame de lookup produtos
def df_produtos(df_h_lk_produtos):
    return (df_h_lk_produtos
            .select(f.upper(f.trim('cd_produto')), f.upper('ds_produto'))
            )


# dataframe join lookups
def df_lk_propostas(df_trata_propostas, df_objeto_financiado, df_produtos):
    df_objeto_financiado = df_objeto_financiado.withColumnRenamed('sg_objeto', 'cd_objeto_financiado')
    return (df_trata_propostas
            .join(df_objeto_financiado, 'cd_objeto_financiado', 'left')
            .join(df_produtos, 'cd_produto', 'left')
            )


# dataframe final do arquivo
def df_final(df_trata_propostas):
    return (df_trata_propostas.select(df_layout_propostas))


# -------------------- INICIO ------------------



# -------------------- FIM INICIO IMPORT LIB ------------------
print "inicio"
df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)

_dados_io = DadosIO('RED_PROPOSTAS')
#_dados_model_prt_io = model.PropostaModel(_dados_io)

_gerenciador = Gerenciador(_dados_io)
df_propostas_fin = _gerenciador.gera_regras()

print 'gravacao...'
try:
    tabela_hive = 'harmonized.h_financeira_propostas'
    _dados_io.gravar_parquet(df_propostas_fin, tabela_hive)
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